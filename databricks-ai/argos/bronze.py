"""Extração Mongo somente find; snapshots/versionamento em Delta."""
import hashlib
import json
from datetime import timedelta
from uuid import uuid4
from .common import VERSION, utcnow, table, sources, config_signature, ensure_schemas, append_frame, assert_delta
from .schemas import BRONZE_DDL, STATE_DDL, RUN_DDL

def canonical_json(value):
    from bson import json_util
    return json_util.dumps(value,json_options=json_util.CANONICAL_JSON_OPTIONS,
                           sort_keys=True,ensure_ascii=False,separators=(',',':'))

def page_query(snapshot, watermark, lower, after):
    clauses=[]
    if not snapshot:
        clauses.append({'updated_at':{'$lte':watermark}})
        if lower is not None:
            clauses.append({'updated_at':{'$gte':lower}})
    if after is not None:
        if snapshot:
            clauses.append({'_id':{'$gt':after[1]}})
        else:
            clauses.append({'$or':[{'updated_at':{'$gt':after[0]}},
                {'updated_at':after[0],'_id':{'$gt':after[1]}}]})
    return {'$and':clauses} if clauses else {}

def bronze_record(doc, source, config, run_id, started):
    if '_id' not in doc:
        raise ValueError('Documento Mongo sem _id.')
    raw=canonical_json(doc)
    payload_hash=hashlib.sha256(raw.encode('utf-8')).hexdigest()
    document_id=doc['_id'] if isinstance(doc['_id'],str) else canonical_json(doc['_id'])
    key='\x1f'.join((config['environment'],source['alias'],document_id,payload_hash,run_id))
    updated=doc.get('updated_at')
    updated=updated if hasattr(updated,'tzinfo') else None
    version=doc.get('version',doc.get('revision'))
    return dict(env=config['environment'],source_alias=source['alias'],source_database=source['database'],
        source_collection=source['collection'],source_group=source['group'],document_id=document_id,
        source_updated_at=updated,source_version=version if isinstance(version,int) and not isinstance(version,bool) else None,
        raw_json=raw,payload_sha256=payload_hash,version_key=hashlib.sha256(key.encode()).hexdigest(),
        extracted_at=utcnow(),ingested_at=utcnow(),run_id=run_id,run_started_at=started,extractor_version=VERSION)

def latest_run(spark, config):
    from pyspark.sql import functions as F
    target=table(config,'bronze','_runs')
    if not spark.catalog.tableExists(target):
        return None
    return (spark.table(target).filter(F.col('env')==config['environment'])
            .orderBy(F.col('run_started_at').desc(),F.col('run_id').desc()).first())

def connection_uri(dbutils, config, group, inline_uri='', inline_a2a_uri=''):
    if group=='a2a' and (config['mongo_a2a_secret_scope'] or inline_a2a_uri):
        scope,key=config['mongo_a2a_secret_scope'],config['mongo_a2a_secret_key']
        uri=dbutils.secrets.get(scope,key) if scope and key else inline_a2a_uri
    else:
        scope,key=config['mongo_secret_scope'],config['mongo_secret_key']
        uri=dbutils.secrets.get(scope,key) if scope and key else inline_uri
    if not isinstance(uri,str) or not uri.startswith(('mongodb://','mongodb+srv://')) or 'PREENCHA' in uri or '<' in uri:
        raise ValueError('Configure mongo_secret_scope/key OU a URI na célula da Bronze. URI não será exibida.')
    return uri

def ingest(spark, dbutils, config, inline_uri='', inline_a2a_uri=''):
    from pymongo import MongoClient, ASCENDING, DESCENDING
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    ensure_schemas(spark,config)
    active=sources(config)
    signature=config_signature(config)
    previous=latest_run(spark,config)
    # Trocar database/collection exige snapshot. A identidade do cluster é o environment.
    changed=previous is not None and previous.source_signature!=signature
    run_id=str(uuid4()); started=utcnow()
    for name,ddl_text in (('_runs',RUN_DDL),('_ingestion_state',STATE_DDL)):
        target=table(config,'bronze',name)
        assert_delta(spark,target)
        spark.sql('CREATE TABLE IF NOT EXISTS '+target+' ('+ddl_text+') USING DELTA')
    clients={}
    try:
        for source in active:
            alias=source['alias']
            # Uma única tabela de Bronze por alias, inclusive A2A (sem colisão entre origens).
            target=table(config,'bronze','b_'+alias)
            assert_delta(spark,target)
            spark.sql('CREATE TABLE IF NOT EXISTS '+target+' ('+BRONZE_DDL+') USING DELTA')
            if source['group'] not in clients:
                uri=connection_uri(dbutils,config,source['group'],inline_uri,inline_a2a_uri)
                clients[source['group']]=MongoClient(uri,tz_aware=True,
                    serverSelectionTimeoutMS=15000,connectTimeoutMS=15000,socketTimeoutMS=60000,
                    appname='argos-databricks-bi',retryReads=True)
                # find é o teste efetivo de autorização; não exige listCollections/admin.
            collection=clients[source['group']][source['database']][source['collection']]
            checkpoint=None
            if previous and not changed:
                committed=(spark.table(table(config,'bronze','_runs'))
                    .filter((F.col('env')==config['environment']) & (F.col('run_started_at')<=previous.run_started_at))
                    .select('env','run_id'))
                checkpoint=(spark.table(table(config,'bronze','_ingestion_state'))
                    .join(committed,['env','run_id'],'inner')
                    .filter((F.col('env')==config['environment']) & (F.col('source_alias')==alias))
                    .orderBy(F.col('run_started_at').desc()).first())
            snapshot=config['force_snapshot'] or changed or checkpoint is None or source['mode']=='snapshot'
            high=None
            if not snapshot:
                # Qualquer documento sem BSON Date força reconciliação completa, não é ignorado.
                malformed=collection.find_one({'updated_at':{'$not':{'$type':'date'}}},{'_id':1})
                newest=collection.find_one({},projection={'updated_at':1},sort=[('updated_at',DESCENDING),('_id',DESCENDING)])
                high=newest.get('updated_at') if newest else None
                if malformed is not None or high is None:
                    snapshot=True
            lower=None
            if not snapshot and checkpoint.watermark is not None:
                lower=checkpoint.watermark-timedelta(minutes=config['lookback_minutes'])
            after=None; documents=0; page_index=0
            def commit_page(rows):
                if not rows:
                    return
                frame=spark.createDataFrame(rows,BRONZE_DDL).dropDuplicates(['version_key'])
                (DeltaTable.forName(spark,target).alias('t').merge(frame.alias('s'),
                    't.version_key=s.version_key').whenNotMatchedInsertAll().execute())
            while True:
                query=page_query(snapshot,high,lower,after)
                ordering=[('_id',ASCENDING)] if snapshot else [('updated_at',ASCENDING),('_id',ASCENDING)]
                page=list(collection.find(query).sort(ordering).limit(config['page_size']).max_time_ms(60000))
                if not page:
                    break
                documents+=len(page)
                if documents>config['max_documents_per_source']:
                    raise RuntimeError('Limite max_documents_per_source excedido para '+alias+'. Aumente o limite; checkpoint não confirmado.')
                rows=[]; byte_count=0
                for doc in page:
                    row=bronze_record(doc,source,config,run_id,started)
                    size=len(row['raw_json'].encode('utf-8'))
                    if size>config['max_page_bytes']:
                        raise ValueError('Documento excede max_page_bytes em '+alias+'; aumente o limite explicitamente.')
                    if rows and byte_count+size>config['max_page_bytes']:
                        commit_page(rows); rows=[]; byte_count=0
                    rows.append(row); byte_count+=size
                    if snapshot:
                        updated=doc.get('updated_at')
                        if hasattr(updated,'tzinfo'):
                            high=max(high,updated) if high is not None else updated
                commit_page(rows)
                after=(page[-1].get('updated_at'),page[-1]['_id']); page_index+=1
            state=dict(env=config['environment'],source_alias=alias,run_id=run_id,run_started_at=started,
                completed_at=utcnow(),is_snapshot=snapshot,watermark=high,documents=documents,source_signature=signature)
            append_frame(spark,spark.createDataFrame([state],STATE_DDL),table(config,'bronze','_ingestion_state'))
            print('Bronze '+alias+': '+str(documents)+' documentos; snapshot='+str(snapshot))
        # Somente este commit libera TODAS as collections para Silver e futuros checkpoints.
        run=dict(env=config['environment'],run_id=run_id,run_started_at=started,completed_at=utcnow(),
            source_signature=signature,source_aliases=[s['alias'] for s in active],extractor_version=VERSION)
        append_frame(spark,spark.createDataFrame([run],RUN_DDL),table(config,'bronze','_runs'))
        print('Bronze concluída. run_id='+run_id)
        return run_id
    except Exception as exc:
        # Erros PyMongo podem incluir hostname. Não registrar URI ou texto do provider.
        if isinstance(exc,(ValueError,RuntimeError)) and not exc.__class__.__module__.startswith('pymongo'):
            raise
        raise RuntimeError('Falha na extração Mongo/Delta ('+type(exc).__name__+'). Confira conexão, rede e permissão find; run não confirmado.') from None
    finally:
        for client in clients.values():
            client.close()
