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
