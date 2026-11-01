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
# Alias, collection de origem, grupo lógico e modo snapshot dos arquivos.
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
    # Identidade interna fixa; não é uma escolha de ambiente.
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
    # Identidade do lote de arquivos e dos metadados lógicos da origem.
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
