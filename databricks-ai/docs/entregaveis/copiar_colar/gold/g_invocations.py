# Databricks notebook source

"""Configuração e operações Delta comuns. Nenhuma credencial é registrada."""
import hashlib
import json
import re
from datetime import datetime, timezone

VERSION = '1.2.0'
DEFAULTS = {
    'catalog': 'workspace', 'bronze_schema': 'bronze_ia', 'silver_schema': 'silver_ia',
    'gold_schema': 'gold_ia', 'environment': 'argos', 'source_database': 'argos_ai',
    'source_a2a_database': '', 'collections_json': '{}',
    'jsonl_path': '/Volumes/workspace/bronze_ia/landing/atual',
    'project_root': '/Workspace/Shared/argos-ai-bi',
    'page_size': '500',
    'max_documents_per_source': '1000000', 'max_page_bytes': '16777216',
    'max_cycle_depth': '100', 'fail_on_quarantine': 'true',
}

SOURCE_DEFINITIONS = (
    ('sessions', 'ai_sessions', 'main', 'snapshot'),
    ('interactions', 'ai_interactions', 'main', 'snapshot'),
    ('messages', 'ai_messages', 'main', 'snapshot'),
    ('execution_events', 'ai_execution_events', 'main', 'snapshot'),
    ('observations', 'ai_observations', 'main', 'snapshot'),
    ('a2a_observations', 'ai_a2a_observations', 'a2a', 'snapshot'),
    ('session_summaries', 'ai_session_summaries', 'main', 'snapshot'),
    ('user_memory', 'ai_user_memory', 'main', 'snapshot'),
    ('report_artifacts', 'ai_report_artifacts', 'main', 'snapshot'),
    ('email_preparations', 'ai_email_workflows', 'main', 'snapshot'),
    ('pending_recipient_resolutions', 'ai_email_workflows', 'main', 'snapshot'),
    ('email_authorizations', 'ai_email_workflows', 'main', 'snapshot'),
    ('email_operation_references', 'ai_email_workflows', 'main', 'snapshot'),
    ('email_reports', 'ai_email_prepared_reports', 'main', 'snapshot'),
    ('email_dispatches', 'ai_email_dispatches', 'main', 'snapshot'),
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
        if key == 'environment':
            continue
        try:
            dbutils.widgets.get(key)
        except Exception:
            dbutils.widgets.text(key, value)
    config = {k: dbutils.widgets.get(k) for k in DEFAULTS if k != 'environment'}

    config['environment'] = 'argos'
    for key in ('catalog','bronze_schema','silver_schema','gold_schema'):
        ident(config[key])
    if not config['source_database'].strip():
        raise ValueError('source_database é obrigatório.')
    validate_jsonl_path(config['jsonl_path'])
    for key in ('page_size','max_documents_per_source','max_page_bytes','max_cycle_depth'):
        config[key] = int(config[key])
    if not 1 <= config['page_size'] <= 1000:
        raise ValueError('page_size deve ser 1..1000.')
    if any(config[k] < 1 for k in ('max_documents_per_source','max_page_bytes','max_cycle_depth')):
        raise ValueError('Limites devem ser positivos.')
    for key in ('fail_on_quarantine',):
        if config[key].lower() not in ('true','false'):
            raise ValueError(key + ' deve ser true ou false.')
        config[key] = config[key].lower() == 'true'
    return config

def validate_jsonl_path(value):
    from pathlib import PurePosixPath
    path = PurePosixPath(value)
    if not value.startswith('/Volumes/') or len(path.parts) < 5 or '..' in path.parts:
        raise ValueError('jsonl_path deve ser um caminho absoluto /Volumes/catalog/schema/volume[/pasta].')
    return str(path)

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
        database=(config['source_a2a_database'] or config['source_database']) if group=='a2a' else config['source_database']
        result.append(dict(alias=alias,collection=collection,group=group,mode=mode,database=database))
    seen=set()
    for item in result:
        key=(item['group'],item['database'],item['collection'])
        if key in seen and item['collection'] != 'ai_email_workflows':
            raise ValueError('Collections duplicadas no mesmo grupo de conexão.')
        seen.add(key)
    if not result:
        raise ValueError('Nenhuma origem habilitada.')
    return result

def config_signature(config):

    identity = dict(sources=sources(config), mode='jsonl', path=config['jsonl_path'])
    return hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()

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

"""Uma consulta por tabela Gold: apenas SELECT e LEFT JOIN sobre Silver."""

GOLD_KEYS={'g_interactions':'interaction_id','g_observations':'observation_id',
           'g_invocations':'invocation_id','g_email_dispatches':'operation_id'}
GOLD_BASE={'g_interactions':'s_interactions','g_observations':'s_observations',
           'g_invocations':'s_invocations','g_email_dispatches':'s_email_dispatches'}

def gold_queries(config):
    def s(name): return table(config,'silver',name)
    return {
      'g_interactions':f'''SELECT i.env, i.interaction_id, i.session_id, i.user_id,
        i.interaction_kind, i.interaction_status, i.created_at, i.updated_at, i.version,
        i.result_status, i.response_kind, i.question_id, i.result_parse_status,
        i.degradation_reasons, i.bronze_version_key, i.source_updated_at,
        i.transform_version, i.published_batch_id,
        s.farm_ids, c.root_interaction_id, c.source_interaction_id, c.link_status
        FROM {s('s_interactions')} i
        LEFT JOIN {s('s_sessions')} s ON i.env=s.env AND i.session_id=s.session_id AND i.user_id=s.user_id
        LEFT JOIN {s('s_cycle_membership')} c ON i.env=c.env AND i.interaction_id=c.interaction_id''',
      'g_observations':f'''SELECT o.*, i.user_id, i.created_at AS interaction_created_at,
        s.farm_ids, h.request_route, h.request_method, h.request_status_code
        FROM {s('s_observations')} o
        LEFT JOIN {s('s_interactions')} i ON o.env=i.env AND o.interaction_id=i.interaction_id
        LEFT JOIN {s('s_sessions')} s ON o.env=s.env AND o.session_id=s.session_id
        LEFT JOIN {s('s_http_requests')} h ON o.env=h.env AND o.correlation_id=h.correlation_id''',
      'g_invocations':f'''SELECT v.*, i.user_id, s.farm_ids FROM {s('s_invocations')} v
        LEFT JOIN {s('s_interactions')} i ON v.env=i.env AND v.interaction_id=i.interaction_id
        LEFT JOIN {s('s_sessions')} s ON v.env=s.env AND v.session_id=s.session_id''',
      'g_email_dispatches':f'''SELECT d.*, r.recipients, r.attachments
        FROM {s('s_email_dispatches')} d
        LEFT JOIN {s('s_email_report_metadata')} r ON d.env=r.env AND d.report_id=r.report_id AND d.requested_by=r.requested_by''',
    }

def silver_publication(spark, config):
    from pyspark.sql import functions as F
    marker=spark.table(table(config,'silver','_publication')).filter(F.col('env')==config['environment']).collect()
    if len(marker)!=1 or marker[0].state!='ready':
        raise ValueError('Silver sem publicação única. Execute 02_silver.')
    return marker[0].published_batch_id

def run_gold(spark, config, name):
    from pyspark.sql import functions as F
    ensure_schemas(spark,config)
    if name not in GOLD_KEYS:
        raise ValueError('Tabela Gold desconhecida.')
    batch=silver_publication(spark,config)

    statement=gold_queries(config)[name]
    frame=spark.sql(statement)
    key=GOLD_KEYS[name]
    assert_unique(frame,['env',key],name)
    base=spark.table(table(config,'silver',GOLD_BASE[name]))
    if frame.count()!=base.count():
        raise ValueError('Junção multiplicou/perdeu registros: '+name)
    if frame.filter(F.col('published_batch_id')!=batch).limit(1).count():
        raise ValueError('Lote inconsistente em '+name)
    write_snapshot(spark,frame,table(config,'gold',name))
    if silver_publication(spark,config)!=batch:
        raise RuntimeError('Silver mudou durante a Gold; execute Job sem concorrência.')
    marker=spark.createDataFrame([(config['environment'],name,batch,utcnow())],
        'env STRING, gold_table STRING, published_batch_id STRING, completed_at TIMESTAMP')
    append_frame(spark,marker,table(config,'gold','_table_publications'))
    print('Gold Delta: '+name+'; lote='+batch)

def validate_pipeline(spark, config):
    from pyspark.sql import functions as F
    ensure_schemas(spark,config)
    batch=silver_publication(spark,config)
    publications=spark.table(table(config,'gold','_table_publications'))
    for name,key in GOLD_KEYS.items():
        target=table(config,'gold',name)
        assert_delta(spark,target)
        frame=spark.table(target)
        assert_unique(frame,['env',key],name)
        if frame.count()!=spark.table(table(config,'silver',GOLD_BASE[name])).count():
            raise ValueError('Cardinalidade incorreta: '+name)
        if frame.filter(F.col('published_batch_id')!=batch).limit(1).count():
            raise ValueError('Lote diferente da Silver: '+name)
        if not publications.filter((F.col('env')==config['environment'])&(F.col('gold_table')==name)
            &(F.col('published_batch_id')==batch)).limit(1).count():
            raise ValueError('Task Gold ainda não confirmou publicação: '+name)

    marker=spark.createDataFrame([(config['environment'],batch,utcnow(),VERSION)],
        'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING')
    write_snapshot(spark,marker,table(config,'gold','_publication'))
    print('Validação aprovada. Lote disponível para dashboards: '+batch)

# COMMAND ----------
config=read_config(dbutils)
run_gold(spark,config,'g_invocations')
