# Databricks notebook source
# MAGIC %pip install pymongo==4.19.0

# COMMAND ----------
# Implementação: argos/common.py
"""Configuração e operações Delta comuns. Nenhuma credencial é registrada."""
import hashlib
import json
import re
from datetime import datetime, timezone

VERSION = '1.0.0'
DEFAULTS = {
    'catalog': 'ai_bi', 'bronze_schema': 'bronze', 'silver_schema': 'silver',
    'gold_schema': 'gold', 'environment': 'local', 'mongo_database': 'argos_ai',
    'mongo_secret_scope': '', 'mongo_secret_key': '',
    'mongo_a2a_secret_scope': '', 'mongo_a2a_secret_key': '',
    'mongo_a2a_database': '', 'collections_json': '{}',
    'page_size': '500', 'lookback_minutes': '5', 'force_snapshot': 'false',
    'max_documents_per_source': '1000000', 'max_page_bytes': '16777216',
    'max_cycle_depth': '100', 'fail_on_quarantine': 'true',
}
# Alias, collection padrão, grupo de conexão, modo seguro.
# Sem updated_at confiável: snapshot, inclusive mensagens/eventos com TTL.
SOURCE_DEFINITIONS = (
    ('sessions', 'ai_sessions', 'main', 'incremental'),
    ('interactions', 'ai_interactions', 'main', 'incremental'),
    ('messages', 'ai_messages', 'main', 'snapshot'),
    ('execution_events', 'ai_execution_events', 'main', 'snapshot'),
    ('observations', 'ai_observations', 'main', 'incremental'),
    ('a2a_observations', 'ai_a2a_observations', 'a2a', 'incremental'),
    ('session_summaries', 'ai_session_summaries', 'main', 'incremental'),
    ('user_memory', 'ai_user_memory', 'main', 'incremental'),
    ('report_artifacts', 'ai_report_artifacts', 'main', 'snapshot'),
    ('email_preparations', 'ai_email_preparations', 'main', 'snapshot'),
    ('pending_recipient_resolutions', 'ai_pending_recipient_resolutions', 'main', 'snapshot'),
    ('email_authorizations', 'ai_email_send_authorizations', 'main', 'snapshot'),
    ('email_operation_references', 'ai_email_operation_references', 'main', 'snapshot'),
    ('email_reports', 'ai_email_prepared_reports', 'main', 'snapshot'),
    ('email_dispatches', 'ai_email_dispatches', 'main', 'incremental'),
)

def utcnow():
    return datetime.now(timezone.utc)

def ident(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value or ''):
        raise ValueError('Identificador inválido; use letras, números e underscore.')
    return '`' + value + '`'

def table(config, layer, name):
    return '.'.join(ident(x) for x in (config['catalog'],config[layer + '_schema'],name))

def read_config(dbutils):
    for key, value in DEFAULTS.items():
        try:
            dbutils.widgets.get(key)
        except Exception:
            dbutils.widgets.text(key, value)
    config = {k: dbutils.widgets.get(k) for k in DEFAULTS}
    for key in ('catalog','bronze_schema','silver_schema','gold_schema'):
        ident(config[key])
    if not config['environment'].strip() or not config['mongo_database'].strip():
        raise ValueError('environment e mongo_database são obrigatórios.')
    for key in ('page_size','lookback_minutes','max_documents_per_source','max_page_bytes','max_cycle_depth'):
        config[key] = int(config[key])
    if not 1 <= config['page_size'] <= 1000 or config['lookback_minutes'] < 0:
        raise ValueError('page_size deve ser 1..1000; lookback_minutes >= 0.')
    if any(config[k] < 1 for k in ('max_documents_per_source','max_page_bytes','max_cycle_depth')):
        raise ValueError('Limites devem ser positivos.')
    for key in ('force_snapshot','fail_on_quarantine'):
        if config[key].lower() not in ('true','false'):
            raise ValueError(key + ' deve ser true ou false.')
        config[key] = config[key].lower() == 'true'
    return config

def sources(config):
    overrides = json.loads(config['collections_json'])
    aliases = {x[0] for x in SOURCE_DEFINITIONS}
    if not isinstance(overrides,dict) or set(overrides)-aliases:
        raise ValueError('collections_json: alias desconhecido.')
    result=[]
    for alias,collection,group,mode in SOURCE_DEFINITIONS:
        override=overrides.get(alias,{})
        if not isinstance(override,dict) or set(override)-{'collection','enabled'}:
            raise ValueError('Override deve ter apenas collection e enabled.')
        if not isinstance(override.get('enabled',True),bool):
            raise ValueError('enabled deve ser booleano JSON.')
        if override.get('enabled',True) is False:
            continue
        collection=override.get('collection',collection)
        if not isinstance(collection,str) or not collection or '\x00' in collection:
            raise ValueError('Nome de collection inválido.')
        database=(config['mongo_a2a_database'] or config['mongo_database']) if group=='a2a' else config['mongo_database']
        result.append(dict(alias=alias,collection=collection,group=group,mode=mode,database=database))
    seen=set()
    for item in result:
        key=(item['group'],item['database'],item['collection'])
        if key in seen:
            raise ValueError('Collections duplicadas no mesmo grupo de conexão.')
        seen.add(key)
    if not result:
        raise ValueError('Nenhuma origem habilitada.')
    return result

def config_signature(config):
    # Sem URI/senha, mas identidade lógica deve mudar se trocar de cluster.
    return hashlib.sha256(json.dumps(sources(config),sort_keys=True).encode()).hexdigest()

def ensure_schemas(spark, config):
    spark.conf.set('spark.sql.session.timeZone','UTC')
    for layer in ('bronze','silver','gold'):
        spark.sql('CREATE SCHEMA IF NOT EXISTS ' + ident(config['catalog'])+'.'+ident(config[layer+'_schema']))
    runs=table(config,'bronze','_runs')
    if spark.catalog.tableExists(runs):
        from pyspark.sql import functions as F
        if spark.table(runs).filter(F.col('env')!=config['environment']).limit(1).count():
            raise ValueError('Namespace já contém outro environment. Use catálogo ou schemas exclusivos por ambiente.')

def assert_delta(spark, target):
    if spark.catalog.tableExists(target):
        # DESCRIBE DETAIL falha para view; nunca substituir view ou formato diferente.
        if spark.sql('DESCRIBE DETAIL '+target).first()['format'].lower() != 'delta':
            raise ValueError('Destino não é Delta: '+target)

def write_snapshot(spark, frame, target):
    assert_delta(spark,target)
    frame.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(target)

def append_frame(spark, frame, target):
    assert_delta(spark,target)
    frame.write.format('delta').mode('append').saveAsTable(target)

def assert_unique(frame, keys, label):
    from pyspark.sql import functions as F
    nulls=None
    for key in keys:
        nulls=F.col(key).isNull() if nulls is None else nulls|F.col(key).isNull()
    if frame.filter(nulls).limit(1).count() or frame.groupBy(*keys).count().filter('count > 1').limit(1).count():
        raise ValueError('PK nula/duplicada: '+label)

# COMMAND ----------
# Implementação: argos/schemas.py
"""Descritores explícitos: produzem DDL Spark e validação Python idênticos."""
def obj(**fields):
    return fields

def arr(item):
    return [item]

def fields(names, kind='STRING'):
    return {name:kind for name in names.split()}

RECIPIENT=obj(**fields('recipient_id name email'))
ATTACHMENT=obj(**fields('filename content_type content_base64 sha256'),size_bytes='BIGINT')
SELECTOR=obj(**fields('kind value'))
PREPARED=obj(**fields('report_id status requested_by subject body'),recipients=arr(RECIPIENT),
             attachments=arr(ATTACHMENT),created_at='TIMESTAMP',expires_at='TIMESTAMP')
LOT=obj(**fields('lot_id lot_code lot_category lot_exception_reason herd_id herd_name herd_breed property_id property_name'),
    lot_active='BOOLEAN',lot_is_exception='BOOLEAN',lot_head_count='BIGINT',lot_open_date='DATE',
    lot_close_date='DATE',slaughter_release_date='DATE',fit_for_slaughter='BOOLEAN')
APPLICATION=obj(**fields('application_id application_kind application_time medicine_name active_ingredient medicine_batch_number unit route goal notes'),
    application_date='DATE',dose='DECIMAL(30,10)',head_count_applied='BIGINT',meat_withdrawal_end='DATE',active='BOOLEAN')
ANIMAL=obj(**fields('animal_id ear_tag category status exception_reason notes'),birth_date='DATE',
    current_weight='DECIMAL(30,10)',exception_date='DATE',exception_end_date='DATE',slaughter_release_date='DATE',fit_for_slaughter='BOOLEAN')
ENVELOPE=obj(**fields('principal_user_id source_service payload_hash signature signature_key_id'),
    authorized_property_ids=arr('STRING'),valid_until='TIMESTAMP',
    report=obj(schema_version='STRING',requested_lot=LOT,applications=arr(APPLICATION),exception_animals=arr(ANIMAL),queried_at='TIMESTAMP',data_as_of='TIMESTAMP'))
USAGE=obj(**fields('availability source'),**fields('input_tokens output_tokens total_tokens cache_read_tokens cache_write_tokens reasoning_tokens','BIGINT'),
          details='MAP<STRING,BIGINT>')
BILLING=obj(**fields('currency cost_source service_tier price_reference'),reported_cost='DECIMAL(38,20)')
ERROR=obj(**fields('type origin code message_sanitized'))
RESOURCE=obj(**fields('name unit source'),quantity='DECIMAL(30,10)')
OBSERVATION=obj(**fields('observation_id schema_version correlation_id parent_observation_id session_id interaction_id service environment service_version operation_kind operation_name operation_phase observation_level agent execution_mode status provider model_requested model_returned model_revision timeout_phase cancellation_state attempt_reason attempt_visibility'),
    **fields('started_at ended_at updated_at','TIMESTAMP'),duration_ms='DOUBLE',timeout_ms='DOUBLE',attempt_number='BIGINT',replayed_result='BOOLEAN',
    http=obj(**fields('method route'),status_code='BIGINT'),
    protocol=obj(**fields('name operation skill task_state error_code')),
    result=obj(**fields('source code'),confirmed='BOOLEAN',recovered='BOOLEAN'),
    error=ERROR,usage=USAGE,billing=BILLING,resources=arr(RESOURCE))
SCHEMAS={
 'sessions':obj(**fields('session_id user_id'),farm_ids=arr('STRING'),created_at='TIMESTAMP',updated_at='TIMESTAMP',
    user_message_count='BIGINT',next_message_sequence='BIGINT',version='BIGINT',lease_fencing_token='BIGINT',
    processing_lease=obj(owner_id='STRING',fencing_token='BIGINT',acquired_at='TIMESTAMP',expires_at='TIMESTAMP')),
 'interactions':obj(**fields('interaction_id session_id user_id deduplication_key interaction_kind sanitized_input input_hash interaction_status result_json result_status response_kind question_id result_parse_status'),
    created_at='TIMESTAMP',updated_at='TIMESTAMP',version='BIGINT',degradation_reasons=arr('STRING')),
 'messages':obj(**fields('message_id session_id user_id interaction_id role kind sanitized_content payload_json source_interaction_id answered_question_id'),
    sequence='BIGINT',created_at='TIMESTAMP',user_message_number='BIGINT',payload_parse_status='STRING'),
 'execution_events':obj(**fields('event_id invocation_id session_id interaction_id user_id target target_name status sanitized_input sanitized_output safe_error_code judge_decision judge_parse_status'),
    created_at='TIMESTAMP',completed_at='TIMESTAMP',attempt='BIGINT',unsupported_claims=arr('STRING'),
    messages=arr(obj(**fields('role sanitized_content tool_name tool_call_id')))),
 'observations':OBSERVATION,
 'session_summaries':obj(session_id='STRING',revision='BIGINT',through_sequence='BIGINT',through_user_message_number='BIGINT',
    **fields('activity pending_activity summary_text'),important_facts=arr(obj(content='STRING',source_references=arr('STRING'))),
    decisions=arr('STRING'),limitations=arr('STRING'),token_count='BIGINT',updated_at='TIMESTAMP'),
 'user_memory':obj(**fields('user_id tone general_goal tone_source_session_id tone_source_interaction_id goal_source_session_id goal_source_interaction_id tone_source_excerpt goal_source_excerpt'),
    revision='BIGINT',**fields('updated_at tone_updated_at goal_updated_at','TIMESTAMP')),
 'report_artifacts':obj(**fields('artifact_id principal_user_id session_id interaction_id task_id'),
    **fields('created_at expires_at purge_at','TIMESTAMP'),envelope=ENVELOPE),
 'email_preparations':obj(**fields('preparation_id principal_user_id session_id interaction_id report_artifact_ref request_fingerprint'),
    **fields('created_at expires_at purge_at','TIMESTAMP'),prepared_report=PREPARED),
 'pending_recipient_resolutions':obj(**fields('question_id principal_user_id session_id interaction_id report_artifact_ref input_type reason_code question_goal'),
    unresolved_selector=SELECTOR,options=arr(obj(option_id='STRING',label='STRING',selector=SELECTOR)),expires_at='TIMESTAMP',purge_at='TIMESTAMP'),
 'email_authorizations':obj(**fields('authorization_id principal_user_id session_id source_interaction_id preparation_reference report_id request_fingerprint prepared_fingerprint recipients_fingerprint origin idempotency_key question_id'),
    **fields('authorized_at expires_at question_expires_at purge_at','TIMESTAMP')),
 'email_operation_references':obj(**fields('authorization_id principal_user_id session_id operation_id last_known_status'),
    **fields('recorded_at purge_at last_known_updated_at','TIMESTAMP')),
 'email_reports':obj(**fields('report_id requested_by subject body source_payload_hash state'),
    recipients=arr(RECIPIENT),attachments=arr(ATTACHMENT),created_at='TIMESTAMP',expires_at='TIMESTAMP'),
 'email_dispatches':obj(**fields('operation_id report_id requested_by idempotency_key state provider_message_id error_code'),
    created_at='TIMESTAMP',updated_at='TIMESTAMP',send_claimed='BOOLEAN'),
}
IDS={
 'sessions':'session_id','interactions':'interaction_id','messages':'message_id','execution_events':'event_id',
 'observations':'observation_id','session_summaries':'session_id','user_memory':'user_id',
 'report_artifacts':'artifact_id','email_preparations':'preparation_id','pending_recipient_resolutions':'question_id',
 'email_authorizations':'authorization_id','email_operation_references':'authorization_id','email_reports':'report_id','email_dispatches':'operation_id',
}

def ddl(spec):
    if isinstance(spec,dict):
        return 'STRUCT<'+','.join('`'+k+'`:'+ddl(v) for k,v in spec.items())+'>'
    if isinstance(spec,list):
        return 'ARRAY<'+ddl(spec[0])+'>'
    return spec

def frame_schema(spark, spec):
    # Public API compatível com Spark 3/4; não depende de StructType.fromDDL.
    return spark.createDataFrame([],','.join('`'+k+'` '+ddl(v) for k,v in spec.items())).schema

BRONZE_DDL='''env STRING, source_alias STRING, source_database STRING, source_collection STRING,
 source_group STRING, document_id STRING, source_updated_at TIMESTAMP, source_version BIGINT,
 raw_json STRING, payload_sha256 STRING, version_key STRING, extracted_at TIMESTAMP,
 ingested_at TIMESTAMP, run_id STRING, run_started_at TIMESTAMP, extractor_version STRING'''
STATE_DDL='''env STRING, source_alias STRING, run_id STRING, run_started_at TIMESTAMP,
 completed_at TIMESTAMP, is_snapshot BOOLEAN, watermark TIMESTAMP, documents BIGINT,
 source_signature STRING'''
RUN_DDL='''env STRING, run_id STRING, run_started_at TIMESTAMP, completed_at TIMESTAMP,
 source_signature STRING, source_aliases ARRAY<STRING>, extractor_version STRING'''

# COMMAND ----------
# Implementação: argos/bronze.py
"""Extração Mongo somente find; snapshots/versionamento em Delta."""
import hashlib
import json
from datetime import timedelta
from uuid import uuid4

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

# COMMAND ----------
# Conexão: preencha os widgets de Secret Scope OU esta URI.
# Não use mongodb://localhost:27017: localhost aqui é o compute do Databricks.
# Nunca publique o notebook depois de preencher credenciais diretamente.
MONGO_URI = 'PREENCHA_A_URI_MONGODB_OU_USE_SECRET_SCOPE'
# Vazio reutiliza a URI principal; preencha só se o A2A usa outro cluster.
MONGO_A2A_URI = ''
config=read_config(dbutils)
ingest(spark,dbutils,config,MONGO_URI,MONGO_A2A_URI)
