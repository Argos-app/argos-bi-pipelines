# Databricks notebook source
# MAGIC %pip install pymongo==4.19.0

# COMMAND ----------
# Implementação: argos/common.py
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
 ingested_at TIMESTAMP, run_id STRING, run_started_at TIMESTAMP, extractor_version STRING,
 source_file STRING, source_record BIGINT'''
STATE_DDL='''env STRING, source_alias STRING, run_id STRING, run_started_at TIMESTAMP,
 completed_at TIMESTAMP, is_snapshot BOOLEAN, watermark TIMESTAMP, documents BIGINT,
 source_signature STRING'''
RUN_DDL='''env STRING, run_id STRING, run_started_at TIMESTAMP, completed_at TIMESTAMP,
 source_signature STRING, source_aliases ARRAY<STRING>, extractor_version STRING'''

# COMMAND ----------
# Implementação: argos/bronze.py
"""Free Edition: snapshots JSONL de Volumes para Bronze Delta."""
import hashlib
import json
from uuid import uuid4

def canonical_json(value):
    from bson import json_util
    return json_util.dumps(value,json_options=json_util.CANONICAL_JSON_OPTIONS,
                           sort_keys=True,ensure_ascii=False,separators=(',',':'))

def bronze_record(doc, source, config, run_id, started, source_file=None, source_record=None):
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
        extracted_at=utcnow(),ingested_at=utcnow(),run_id=run_id,run_started_at=started,extractor_version=VERSION,source_file=source_file,source_record=source_record)

def latest_run(spark, config):
    from pyspark.sql import functions as F
    target=table(config,'bronze','_runs')
    if not spark.catalog.tableExists(target):
        return None
    return (spark.table(target).filter(F.col('env')==config['environment'])
            .orderBy(F.col('run_started_at').desc(),F.col('run_id').desc()).first())

def ingest(spark, config):
    return ingest_jsonl(spark, config)

def ensure_bronze_table(spark, target):
    assert_delta(spark, target)
    spark.sql('CREATE TABLE IF NOT EXISTS ' + target + ' (' + BRONZE_DDL + ') USING DELTA')
    existing = set(spark.table(target).schema.fieldNames())
    # Migração aditiva de metadados para tabelas criadas antes do modo JSONL.
    for name, kind in (('source_file', 'STRING'), ('source_record', 'BIGINT')):
        if name not in existing:
            spark.sql('ALTER TABLE ' + target + ' ADD COLUMNS (' + name + ' ' + kind + ')')

def merge_page(spark, target, rows):
    if not rows:
        return
    view = '_argos_page_' + uuid4().hex
    spark.createDataFrame(rows, BRONZE_DDL).dropDuplicates(['version_key']).createOrReplaceTempView(view)
    try:
        spark.sql('MERGE INTO ' + target + ' t USING ' + view + ' s ON t.version_key=s.version_key '
                  'WHEN NOT MATCHED THEN INSERT *')
    finally:
        spark.catalog.dropTempView(view)

def iter_jsonl_documents(path, max_bytes):
    from bson import json_util
    options = json_util.CANONICAL_JSON_OPTIONS.with_options(tz_aware=True)
    with open(path, 'rb') as stream:
        position = 0
        while True:
            line = stream.readline(max_bytes + 2)
            if not line:
                break
            position += 1
            if len(line.rstrip(b'\r\n')) > max_bytes:
                raise ValueError('Linha JSONL excede max_page_bytes; registro ' + str(position))
            if not line.strip():
                continue
            try:
                document = json_util.loads(line.decode('utf-8'), json_options=options)
                if not isinstance(document, dict) or '_id' not in document:
                    raise ValueError()
            except Exception:
                raise ValueError('JSONL inválido; registro ' + str(position) + '. Use Extended JSON, um documento por linha.') from None
            yield position, document

WORKFLOW_TYPES = {
    'email_preparations': 'preparation',
    'pending_recipient_resolutions': 'pending_resolution',
    'email_authorizations': 'authorization',
    'email_operation_references': 'operation_reference',
}

def ingest_jsonl(spark, config):
    from pathlib import Path
    active = sources(config)
    files = {s['alias']: Path(config['jsonl_path']) / (('email_workflows' if s['alias'] in WORKFLOW_TYPES else s['alias']) + '.jsonl') for s in active}
    missing = [alias for alias, path in files.items() if not path.is_file()]
    if missing:
        raise ValueError('JSONL ausente para: ' + ', '.join(missing) + '. Cada origem exige snapshot completo, inclusive arquivo vazio.')
    ensure_schemas(spark, config)
    signature = config_signature(config)
    run_id, started = str(uuid4()), utcnow()
    for name, ddl_text in (('_runs', RUN_DDL), ('_ingestion_state', STATE_DDL)):
        target = table(config, 'bronze', name)
        assert_delta(spark, target)
        spark.sql('CREATE TABLE IF NOT EXISTS ' + target + ' (' + ddl_text + ') USING DELTA')
    for source in active:
        alias = source['alias']
        target = table(config, 'bronze', 'b_' + alias)
        ensure_bronze_table(spark, target)
        rows, byte_count, documents, high = [], 0, 0, None
        for position, doc in iter_jsonl_documents(files[alias], config['max_page_bytes']):
            if alias in WORKFLOW_TYPES:
                if doc.get('record_type') not in WORKFLOW_TYPES.values():
                    raise ValueError('record_type desconhecido em email_workflows; run não confirmado.')
                if doc['record_type'] != WORKFLOW_TYPES[alias]:
                    continue
            documents += 1
            if documents > config['max_documents_per_source']:
                raise RuntimeError('Limite max_documents_per_source excedido para ' + alias + '; run não confirmado.')
            row = bronze_record(doc, source, config, run_id, started, str(files[alias]), position)
            size = len(row['raw_json'].encode('utf-8'))
            if size > config['max_page_bytes']:
                raise ValueError('Documento excede max_page_bytes em ' + alias)
            if rows and (len(rows) >= config['page_size'] or byte_count + size > config['max_page_bytes']):
                merge_page(spark, target, rows)
                rows, byte_count = [], 0
            rows.append(row)
            byte_count += size
            updated = row['source_updated_at']
            if updated is not None:
                high = max(high, updated) if high is not None else updated
        merge_page(spark, target, rows)
        state = dict(env=config['environment'], source_alias=alias, run_id=run_id, run_started_at=started,
            completed_at=utcnow(), is_snapshot=True, watermark=high, documents=documents, source_signature=signature)
        append_frame(spark, spark.createDataFrame([state], STATE_DDL), table(config, 'bronze', '_ingestion_state'))
        print('Bronze JSONL ' + alias + ': ' + str(documents) + ' documentos; snapshot=True')
    run = dict(env=config['environment'], run_id=run_id, run_started_at=started, completed_at=utcnow(),
        source_signature=signature, source_aliases=[s['alias'] for s in active], extractor_version=VERSION)
    append_frame(spark, spark.createDataFrame([run], RUN_DDL), table(config, 'bronze', '_runs'))
    print('Bronze JSONL concluída. run_id=' + run_id)
    return run_id

# COMMAND ----------
# Snapshot JSONL completo em jsonl_path (/Volumes).
config=read_config(dbutils)
ingest(spark,config)
