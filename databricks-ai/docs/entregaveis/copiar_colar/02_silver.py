# Databricks notebook source
# MAGIC %pip install pymongo==4.19.0

# COMMAND ----------

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

"""Parser BSON independente de Spark; valida sem substituir desconhecido por zero."""
import json
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import UUID

def typed_value(value, spec, path, errors):
    if value is None:
        return None
    if isinstance(spec,dict):
        if not isinstance(value,dict):
            errors.append(path+':object'); return None
        return {k:typed_value(value.get(k),v,path+'.'+k,errors) for k,v in spec.items()}
    if isinstance(spec,list):
        if not isinstance(value,(list,tuple)):
            errors.append(path+':array'); return None
        if any(x is None for x in value):
            errors.append(path+':null_element')
        return [typed_value(x,spec[0],path+'[]',errors) for x in value]
    try:
        if spec=='STRING':
            if isinstance(value,(str,UUID)):
                return str(value)
            if path.endswith(('.code','.error_code')) and isinstance(value,int) and not isinstance(value,bool):
                return str(value)
            raise ValueError()
        if spec=='BIGINT':
            if not isinstance(value,int) or isinstance(value,bool) or not -(2**63)<=value<2**63:
                raise ValueError()
            return int(value)
        if spec=='DOUBLE':
            if isinstance(value,bool) or not isinstance(value,(int,float,Decimal)) or not math.isfinite(float(value)):
                raise ValueError()
            return float(value)
        if spec=='BOOLEAN':
            if not isinstance(value,bool): raise ValueError()
            return value
        if spec=='TIMESTAMP':
            if isinstance(value,str):
                value=datetime.fromisoformat(value.replace('Z','+00:00'))
            if not isinstance(value,datetime) or value.tzinfo is None or value.utcoffset().total_seconds()!=0:
                raise ValueError()
            return value.astimezone(timezone.utc)
        if spec=='DATE':
            if isinstance(value,datetime): raise ValueError()
            return date.fromisoformat(value) if isinstance(value,str) else value if isinstance(value,date) else (_ for _ in ()).throw(ValueError())
        if spec.startswith('DECIMAL'):
            from bson.decimal128 import Decimal128
            precision,scale=map(int,re.findall(r'\d+',spec))
            if isinstance(value,Decimal128): value=value.to_decimal()
            if not isinstance(value,(Decimal,str,int)) or isinstance(value,bool): raise ValueError()
            result=Decimal(value)
            if not result.is_finite(): raise ValueError()

            exponent=result.as_tuple().exponent
            if max(0,-exponent)>scale or result.adjusted()+1>precision-scale:
                raise ValueError()
            return result
        if spec=='MAP<STRING,BIGINT>':
            if not isinstance(value,dict): raise ValueError()
            return {str(k):typed_value(v,'BIGINT',path+'.'+str(k),errors) for k,v in value.items()}
    except (ValueError,TypeError,InvalidOperation,OverflowError):
        errors.append(path+':'+spec); return None
    raise ValueError('Tipo de schema não implementado: '+str(spec))

def require_uuid(record, key, errors, optional=False):
    value=record.get(key)
    if optional and value is None: return
    try:
        if not isinstance(value,str) or str(UUID(value))!=value.lower(): raise ValueError()
    except (ValueError,TypeError,AttributeError):
        errors.append(key+':uuid')

def parse_embedded(value):
    if not isinstance(value,str): return None
    try:
        result=json.loads(value)
        return result if isinstance(result,dict) else None
    except (ValueError,TypeError):
        return None

def prepare_document(alias, doc):
    doc=dict(doc)
    doc[IDS[alias]]=doc.get('_id')
    if alias == 'email_operation_references' and isinstance(doc.get('_id'), str):
        doc[IDS[alias]] = doc['_id'].removeprefix('operation:')
    if alias=='interactions':
        doc['interaction_kind']=doc.get('kind'); doc['interaction_status']=doc.get('status')
        result=parse_embedded(doc.get('result_json'))
        valid=(result is not None and result.get('schema_version')=='1'
            and result.get('status') in ('completed','degraded') and isinstance(result.get('response'),dict)
            and result['response'].get('kind') in ('answer','question')
            and isinstance(result.get('degradation_reasons',[]),list)
            and all(isinstance(x,str) for x in result.get('degradation_reasons',[])))
        doc['result_parse_status']='absent' if doc.get('result_json') is None else 'valid' if valid else 'invalid'
        if valid:
            doc.update(result_status=result['status'],response_kind=result['response']['kind'],
                question_id=result['response'].get('question_id'),degradation_reasons=result.get('degradation_reasons',[]))
    if alias=='messages':
        value=parse_embedded(doc.get('payload_json'))
        valid=value is not None
        doc['payload_parse_status']='absent' if doc.get('payload_json') is None else 'valid' if valid else 'invalid'
        if doc.get('role')=='user' and doc.get('kind')=='user_input' and valid and value.get('kind')=='answer':
            doc['source_interaction_id']=value.get('interaction_id')
            doc['answered_question_id']=value.get('question_id')
    if alias=='execution_events':
        doc['judge_parse_status']='not_applicable'
        if doc.get('target')=='agent' and doc.get('target_name')=='judge' and doc.get('status')!='started':
            result=parse_embedded(doc.get('sanitized_output')) if doc.get('status')=='succeeded' else None
            claims=result.get('unsupported_claims',[]) if result else None
            valid=(result is not None and result.get('decision') in ('approved','revise','rejected')
                and isinstance(claims,list) and all(isinstance(x,str) for x in claims)
                and not (result.get('decision')=='approved' and claims))
            doc['judge_parse_status']='valid' if valid else 'invalid'
            if valid: doc.update(judge_decision=result['decision'],unsupported_claims=claims)
    return doc

def validate_record(alias, record, errors):
    require_uuid(record,IDS[alias],errors)
    dates={
        'sessions':('created_at','updated_at'),'interactions':('created_at','updated_at'),
        'messages':('created_at',),'execution_events':('created_at',),
        'session_summaries':('updated_at',),'user_memory':('updated_at',),
        'report_artifacts':('created_at','expires_at','purge_at'),
        'email_preparations':('created_at','expires_at','purge_at'),
        'pending_recipient_resolutions':('expires_at','purge_at'),
        'email_authorizations':('authorized_at','expires_at','purge_at'),
        'email_operation_references':('recorded_at','purge_at'),
        'email_reports':('created_at','expires_at'),'email_dispatches':('created_at','updated_at'),
    }
    for key in dates.get(alias,()):
        if record.get(key) is None: errors.append(key+':required')
    for key in ('session_id','interaction_id','user_id','principal_user_id','invocation_id'):
        if key in record and key!=IDS[alias]:
            require_uuid(record,key,errors,optional=alias=='observations')
    if alias=='observations':
        required=('schema_version','service','environment','operation_kind','operation_name','observation_level','execution_mode','started_at','updated_at','status')
        for key in required:
            if record.get(key) is None or record.get(key)=='': errors.append(key+':required')
        enums={
            'schema_version':{'1','2'},'status':{'started','succeeded','failed','timeout','cancelled','rejected'},
            'operation_kind':{'http','interaction','graph','agent','model','embedding','tool','mcp','a2a','workflow','storage','external'},
            'observation_level':{'local_operation','sdk_invocation','transport_attempt'},'execution_mode':{'real','mock','unavailable'},
            'operation_phase':{'initialization','execution','validation','recovery'},
            'attempt_reason':{'initial','retry_transient','fallback','repair','judge_revision','source_renewal','unknown'},
            'attempt_visibility':{'individual','internal_retries_unknown'},'cancellation_state':{'requested','observed','confirmed'},
        }
        for key,values in enums.items():
            if record.get(key) is not None and record[key] not in values: errors.append(key+':enum')
        for key in ('correlation_id','parent_observation_id'):
            require_uuid(record,key,errors,optional=True)
        terminal=record.get('status') not in (None,'started')
        if terminal and (record.get('ended_at') is None or record.get('duration_ms') is None): errors.append('terminal:missing_end')
        if record.get('status')=='started' and (record.get('ended_at') is not None or record.get('duration_ms') is not None): errors.append('started:has_end')
        if record.get('status') in ('failed','timeout') and record.get('error') is None: errors.append('failure:missing_error')
        usage=record.get('usage')
        if usage:
            if usage['availability'] not in ('returned','partial','absent','not_applicable'): errors.append('usage.availability:enum')
            counts=[v for k,v in usage.items() if k.endswith('_tokens') and v is not None]
            if any(v<0 for v in counts) or any(v is None or v<0 for v in (usage.get('details') or {}).values()): errors.append('usage:negative')
            if usage['availability']=='absent' and (counts or usage.get('details')): errors.append('usage.absent:has_counts')
        billing=record.get('billing')
        if billing and billing.get('reported_cost') is not None:
            if billing['reported_cost']<0 or not re.fullmatch('[A-Z]{3}',billing.get('currency') or '') or billing.get('cost_source') not in ('provider','configured_rate'):
                errors.append('billing:invalid')
        if billing and billing.get('reported_cost') is None and (billing.get('currency') is not None or billing.get('cost_source') is not None):
            errors.append('billing:metadata_without_cost')
        error=record.get('error')
        if error and (not error.get('type') or error.get('origin') not in ('application','provider','transport','protocol','validation','storage','unknown')):
            errors.append('error:invalid')
        http=record.get('http')
        if http and http.get('status_code') is not None and not 100<=http['status_code']<=599: errors.append('http.status_code:range')
        for key in ('duration_ms','timeout_ms'):
            if record.get(key) is not None and (record[key]<0 or (key=='timeout_ms' and record[key]==0)): errors.append(key+':negative')
        if record.get('attempt_number') is not None and record['attempt_number']<1: errors.append('attempt_number:positive')
        for resource in record.get('resources') or []:
            if resource and (resource.get('quantity') is None or resource['quantity']<0
                or not resource.get('name') or not resource.get('unit') or resource.get('source') not in ('provider','sdk','application')):
                errors.append('resources:invalid')
        if record.get('updated_at') and record.get('ended_at') and record['updated_at']<record['ended_at']:
            errors.append('updated_at:before_end')
    if alias=='sessions':
        farm_ids=record.get('farm_ids')
        if not farm_ids or len(farm_ids)!=len(set(farm_ids)): errors.append('farm_ids:empty_or_duplicate')
        for value in farm_ids or []: require_uuid({'farm':value},'farm',errors)
    if alias=='interactions':
        if record['interaction_kind'] not in ('message','answer'): errors.append('interaction_kind:enum')
        if record['interaction_status'] not in ('accepted','running','completed','degraded','failed','interrupted'): errors.append('interaction_status:enum')
    if alias=='execution_events':
        if record['status'] not in ('started','succeeded','failed') or record['target'] not in ('agent','tool'): errors.append('execution:enum')
        if record['status']!='started' and record.get('completed_at') is None: errors.append('execution:missing_end')
        if record['status']=='failed' and not record.get('safe_error_code'): errors.append('execution:missing_error')
        if record['status']=='started' and record.get('completed_at') is not None: errors.append('execution:started_has_end')
    if alias=='messages':
        if record['role'] not in ('user','assistant') or record['kind'] not in ('user_input','assistant_answer','clarification_question'):
            errors.append('message:enum')
    if alias=='email_dispatches' and record['state'] not in ('pending','accepted','partial','failed','unknown'): errors.append('state:enum')
    for key in ('sequence','version','revision','attempt'):
        if key in record and record[key] is not None and record[key]<1: errors.append(key+':positive')
    for start,end in (('started_at','ended_at'),('created_at','updated_at'),('created_at','completed_at')):
        if record.get(start) and record.get(end) and record[end]<record[start]: errors.append(end+':before_start')

def parse_record(alias, raw):
    from bson import json_util
    alias='observations' if alias=='a2a_observations' else alias
    errors=[]
    try:
        doc=json_util.loads(raw,json_options=json_util.CANONICAL_JSON_OPTIONS.with_options(tz_aware=True))
        if not isinstance(doc,dict): raise ValueError()
        doc=prepare_document(alias,doc)
        record=typed_value(doc,SCHEMAS[alias],alias,errors)
        validate_record(alias,record,errors)

        import hashlib
        semantic=dict(doc); semantic.pop('updated_at',None)
        semantic_hash=hashlib.sha256(json_util.dumps(semantic,json_options=json_util.CANONICAL_JSON_OPTIONS,
            sort_keys=True,separators=(',',':')).encode()).hexdigest()
        return {'record':record,'errors':sorted(set(errors)),'semantic_hash':semantic_hash}
    except Exception:

        return {'record':None,'errors':['document:unparseable'],'semantic_hash':None}

# COMMAND ----------

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

"""Materialização temporária em Delta usando somente APIs Spark Connect."""
import warnings
from uuid import uuid4

class DeltaStage:
    """Uma tabela imutável por etapa; nunca sobrescreve uma tabela ainda lida."""
    def __init__(self, spark, config):
        self.spark, self.config = spark, config
        self.prefix = '_work_' + uuid4().hex
        self.targets = []

    def materialize(self, frame):
        target = table(self.config, 'silver', self.prefix + '_' + str(len(self.targets)))
        if self.spark.catalog.tableExists(target):
            raise RuntimeError('Colisão de staging; nenhuma tabela existente será substituída.')
        self.targets.append(target)
        frame.write.format('delta').mode('errorifexists').saveAsTable(target)
        return self.spark.table(target)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        for target in reversed(self.targets):
            try:
                self.spark.sql('DROP TABLE IF EXISTS ' + target)
            except Exception:
                warnings.warn('Staging Delta retido para limpeza posterior: ' + target, RuntimeWarning)
        return False

# COMMAND ----------

"""Estado corrente, parsing tipado, quarentena e relacionamentos estruturais."""

def committed_bronze(spark, config, alias, run):
    from pyspark.sql import functions as F
    states=(spark.table(table(config,'bronze','_ingestion_state'))
        .filter((F.col('env')==config['environment']) & (F.col('source_alias')==alias)))
    commits=(spark.table(table(config,'bronze','_runs'))
        .filter((F.col('env')==config['environment']) & (F.col('run_started_at')<=run.run_started_at))
        .select('env','run_id'))
    states=states.join(commits,['env','run_id'],'inner')
    baseline=states.filter('is_snapshot').orderBy(F.col('run_started_at').desc()).first()
    if baseline is None:
        raise ValueError('Origem sem snapshot inicial confirmado: '+alias)
    valid=states.filter(F.col('run_started_at')>=baseline.run_started_at).select('env','run_id').distinct()
    return spark.table(table(config,'bronze','b_'+alias)).join(valid,['env','run_id'],'inner')

def quarantine(spark, config, frame, run_id):
    from pyspark.sql import functions as F
    q=(frame.select('env','source_alias','document_id',F.col('version_key').alias('bronze_version_key'),
        F.col('p.errors').alias('rule_ids'))
       .withColumn('detected_at',F.current_timestamp()).withColumn('parser_version',F.lit(VERSION))
       .withColumn('published_batch_id',F.lit(run_id)))
    n=q.count()
    if n:
        append_frame(spark,q,table(config,'silver','q_records'))
    return n

def parse_source(spark, config, alias, raw, run, stage):
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StructType,StructField,StringType,ArrayType
    canonical='observations' if alias=='a2a_observations' else alias
    schema=frame_schema(spark,SCHEMAS[canonical])
    result_schema=StructType([StructField('record',schema),StructField('errors',ArrayType(StringType())),StructField('semantic_hash',StringType())])
    parser=F.udf(lambda text:parse_record(canonical,text),result_schema)
    parsed=stage.materialize(raw.withColumn('p',parser('raw_json')))
    count_bad=quarantine(spark,config,parsed.filter(F.size('p.errors')>0),run.run_id)
    if count_bad and config['fail_on_quarantine']:
        raise ValueError('Quarentena em '+alias+': '+str(count_bad)+' versões. Consulte silver.q_records; nenhuma Gold será publicada.')
    valid=parsed.filter(F.size('p.errors')==0)

    if canonical in ('observations','execution_events'):
        terminal=valid.filter(F.col('p.record.status')!='started') if canonical=='observations' else valid
        conflicts=(terminal.groupBy('env','document_id').agg(F.countDistinct('p.semantic_hash').alias('variants'))
                   .filter('variants>1').select('env','document_id'))
        conflict_rows=valid.join(conflicts,['env','document_id'],'inner')
        marked=conflict_rows.withColumn('p',F.struct(F.col('p.record').alias('record'),
            F.array(F.lit('identity:conflicting_content')).alias('errors'),F.col('p.semantic_hash').alias('semantic_hash')))
        conflict_count=quarantine(spark,config,marked,run.run_id)
        if conflict_count and config['fail_on_quarantine']:
            raise ValueError('Conteúdo divergente para mesma identidade em '+alias+'. Consulte q_records.')
        valid=valid.join(conflicts,['env','document_id'],'left_anti')
    ordering=[]
    if canonical=='observations':
        ordering.append(F.when(F.col('p.record.status')=='started',0).otherwise(1).desc())
    ordering.extend([F.col('source_version').desc_nulls_last(),F.col('source_updated_at').desc_nulls_last(),
                     F.col('run_started_at').desc(),F.col('version_key').desc()])
    window=Window.partitionBy('env','document_id').orderBy(*ordering)
    current=valid.withColumn('_rn',F.row_number().over(window)).filter('_rn=1')
    return current.select('env','p.record.*','source_alias','source_collection','source_database',
        F.col('version_key').alias('bronze_version_key'),'source_updated_at',
        F.lit(VERSION).alias('transform_version'),F.lit(run.run_id).alias('published_batch_id'))

def build_invocations(spark, config):
    from pyspark.sql import functions as F
    e=spark.table(table(config,'silver','s_execution_events'))
    assert_unique(e,['env','invocation_id','status'],'eventos por invocação/estado')
    z=e.filter("status IN ('succeeded','failed')")
    assert_unique(z,['env','invocation_id'],'término de invocação')
    a=e.filter("status='started'")
    keys=e.select('env','invocation_id').distinct()

    paired=a.alias('a').join(z.alias('z'),['env','invocation_id'],'inner')
    mismatch=None
    for field in ('session_id','interaction_id','target','target_name','attempt'):
        expr=~F.col('a.'+field).eqNullSafe(F.col('z.'+field))
        mismatch=expr if mismatch is None else mismatch|expr
    if paired.filter(mismatch).limit(1).count():
        raise ValueError('Invocação com início/término incompatíveis.')
    result=(keys.alias('k').join(a.alias('a'),['env','invocation_id'],'left')
        .join(z.alias('z'),['env','invocation_id'],'left')
        .select('env','invocation_id',*[F.coalesce(F.col('a.'+k),F.col('z.'+k)).alias(k)
             for k in ('interaction_id','session_id','target','target_name','attempt')],
            F.col('a.event_id').alias('start_event_id'),F.col('z.event_id').alias('end_event_id'),
            F.coalesce(F.col('a.created_at'),F.col('z.created_at')).alias('started_at'),F.col('z.completed_at').alias('ended_at'),
            F.col('z.status').alias('terminal_status'),F.col('z.safe_error_code').alias('safe_error_code'),
            F.col('z.judge_decision').alias('judge_decision'),F.col('z.judge_parse_status').alias('judge_parse_status'),
            F.col('z.unsupported_claims').alias('unsupported_claims'),
            F.coalesce(F.col('z.published_batch_id'),F.col('a.published_batch_id')).alias('published_batch_id')))
    if result.filter('ended_at < started_at').limit(1).count():
        raise ValueError('Fim de invocação anterior ao início.')
    return result

def build_membership(spark, config, stage):
    from pyspark.sql import functions as F
    i=spark.table(table(config,'silver','s_interactions'))
    m=spark.table(table(config,'silver','s_messages')).filter("role='user' AND kind='user_input'")
    candidates=m.groupBy('env','interaction_id').agg(F.count('*').alias('message_count'),
        F.first('source_interaction_id').alias('source_interaction_id'),F.first('answered_question_id').alias('answered_question_id'))

    links=(i.alias('i').join(candidates.alias('m'),['env','interaction_id'],'left')
       .select('env','interaction_id','i.session_id','i.user_id','i.interaction_kind','i.published_batch_id',
               'm.source_interaction_id','m.answered_question_id','m.message_count'))
    parent=i.select(F.col('env').alias('penv'),F.col('interaction_id').alias('pid'),F.col('session_id').alias('psession'),
        F.col('user_id').alias('puser'),F.col('response_kind').alias('pkind'),F.col('question_id').alias('pquestion'))
    linked=links.join(parent,(links.env==parent.penv)&(links.source_interaction_id==parent.pid),'left')
    valid_parent=((F.col('message_count')==1)&(F.col('pid').isNotNull())
        &(F.col('session_id')==F.col('psession'))&(F.col('user_id')==F.col('puser'))
        &(F.col('pkind')=='question')&(F.col('answered_question_id')==F.col('pquestion')))
    resolved=(linked.select('env','interaction_id','source_interaction_id','published_batch_id',
        F.when(F.col('interaction_kind')=='message',F.col('interaction_id')).alias('root_interaction_id'),
        F.when((F.col('interaction_kind')=='answer') & valid_parent,F.col('pid')).alias('valid_parent_id')))
    resolved=stage.materialize(resolved)
    for _ in range(config['max_cycle_depth']):
        parents=resolved.select(F.col('env').alias('pe'),F.col('interaction_id').alias('pi'),F.col('root_interaction_id').alias('pr'))
        joined=resolved.join(parents,(resolved.env==parents.pe)&(resolved.valid_parent_id==parents.pi),'left')
        if not joined.filter('root_interaction_id IS NULL AND pr IS NOT NULL').limit(1).count():
            break
        nxt=joined.select('env','interaction_id','source_interaction_id','published_batch_id','valid_parent_id',
            F.coalesce('root_interaction_id','pr').alias('root_interaction_id'))
        resolved=stage.materialize(nxt)

    branches=(resolved.filter('valid_parent_id IS NOT NULL').groupBy('env','valid_parent_id').count()
              .filter('count>1').select('env',F.col('valid_parent_id').alias('bid')))
    bad_roots=(resolved.join(branches,(resolved.env==branches.env)&(resolved.interaction_id==branches.bid),'inner')
        .select(resolved.env,resolved.root_interaction_id).distinct().withColumn('ambiguous',F.lit(True)))
    return (resolved.join(bad_roots,['env','root_interaction_id'],'left')
        .select('env','interaction_id','root_interaction_id','source_interaction_id','published_batch_id',
            F.when(F.col('root_interaction_id').isNull(),'unknown').when(F.col('ambiguous'),'ambiguous').otherwise('valid').alias('link_status')))

def _run_silver(spark, config, stage):
    from pyspark.sql import functions as F
    ensure_schemas(spark,config)
    spark.sql('CREATE TABLE IF NOT EXISTS '+table(config,'silver','q_records')+''' (
        env STRING, source_alias STRING, document_id STRING, bronze_version_key STRING,
        rule_ids ARRAY<STRING>, detected_at TIMESTAMP, parser_version STRING, published_batch_id STRING
    ) USING DELTA''')

    spark.sql('CREATE TABLE IF NOT EXISTS '+table(config,'gold','_table_publications')+''' (
        env STRING, gold_table STRING, published_batch_id STRING, completed_at TIMESTAMP
    ) USING DELTA''')
    run=latest_run(spark,config)
    if run is None or run.source_signature!=config_signature(config):
        raise ValueError('Execute Bronze com a mesma configuração antes da Silver.')
    pending=spark.createDataFrame([(config['environment'],run.run_id,utcnow(),VERSION,'running')],
        'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING, state STRING')
    write_snapshot(spark,pending,table(config,'silver','_publication'))
    active={s['alias'] for s in sources(config)}
    observation_frames=[]
    for alias in [x[0] for x in SOURCE_DEFINITIONS]:
        canonical='observations' if alias=='a2a_observations' else alias
        if alias in active:
            frame=parse_source(spark,config,alias,committed_bronze(spark,config,alias,run),run,stage)
        else:
            spec=dict(env='STRING',**SCHEMAS[canonical],source_alias='STRING',source_collection='STRING',source_database='STRING',
                bronze_version_key='STRING',source_updated_at='TIMESTAMP',transform_version='STRING',published_batch_id='STRING')
            frame=spark.createDataFrame([],frame_schema(spark,spec))
        if canonical=='observations':
            observation_frames.append(frame); continue
        assert_unique(frame,['env',IDS[canonical]],canonical)
        write_snapshot(spark,frame,table(config,'silver','s_'+canonical))
    obs=observation_frames[0].unionByName(observation_frames[1])
    assert_unique(obs,['env','observation_id'],'observations global API/A2A')
    write_snapshot(spark,obs,table(config,'silver','s_observations'))

    events=spark.table(table(config,'silver','s_execution_events'))
    assert_unique(events,['env','session_id','interaction_id','event_id'],'execution_events')
    messages=spark.table(table(config,'silver','s_messages'))
    assert_unique(messages,['env','session_id','sequence'],'messages.sequence')
    event_messages=(events.select('env','event_id',F.posexplode('messages').alias('message_pos','message'))
        .select('env','event_id','message_pos','message.*'))
    write_snapshot(spark,event_messages,table(config,'silver','s_event_messages'))
    resources=(spark.table(table(config,'silver','s_observations'))
        .select('env','observation_id',F.posexplode('resources').alias('resource_pos','resource'))
        .select('env','observation_id','resource_pos','resource.*'))
    write_snapshot(spark,resources,table(config,'silver','s_observation_resources'))
    invocations=build_invocations(spark,config)
    assert_unique(invocations,['env','invocation_id'],'invocations')
    write_snapshot(spark,invocations,table(config,'silver','s_invocations'))
    membership=build_membership(spark,config,stage)
    assert_unique(membership,['env','interaction_id'],'cycle_membership')
    write_snapshot(spark,membership,table(config,'silver','s_cycle_membership'))
    observations=spark.table(table(config,'silver','s_observations'))
    http=(observations.filter("source_alias='observations' AND operation_kind='http' AND operation_name='http.request' AND correlation_id IS NOT NULL")
        .select('env','correlation_id','observation_id',F.col('http.route').alias('request_route'),
            F.col('http.method').alias('request_method'),F.col('http.status_code').alias('request_status_code'),
            'started_at','ended_at','duration_ms','status','published_batch_id'))
    assert_unique(http,['env','correlation_id'],'http_requests')
    write_snapshot(spark,http,table(config,'silver','s_http_requests'))

    reports=spark.table(table(config,'silver','s_email_reports'))
    public=(reports.select('env','report_id','requested_by','published_batch_id',
        F.transform('recipients',lambda x:F.struct(x.recipient_id.alias('recipient_id'))).alias('recipients'),
        F.transform('attachments',lambda x:F.struct(x.filename.alias('filename'),x.content_type.alias('content_type'),
            x.sha256.alias('sha256'),x.size_bytes.alias('size_bytes'))).alias('attachments')))
    write_snapshot(spark,public,table(config,'silver','s_email_report_metadata'))

    interactions=spark.table(table(config,'silver','s_interactions'))
    sessions=spark.table(table(config,'silver','s_sessions'))
    issues=[]
    relations=[('s_interactions','interaction_id','session_id',sessions,'session_id','user_id'),
        ('s_observations','observation_id','interaction_id',interactions,'interaction_id','session_id'),
        ('s_execution_events','event_id','interaction_id',interactions,'interaction_id','session_id'),
        ('s_messages','message_id','interaction_id',interactions,'interaction_id','session_id')]
    for entity,key,fk,right,pk,consistent in relations:
        left=spark.table(table(config,'silver',entity)).alias('a')
        other=right.alias('b')
        linked=left.join(other,(F.col('a.env')==F.col('b.env'))&(F.col('a.'+fk)==F.col('b.'+pk)),'left')
        contradiction=linked.filter(F.col('b.'+pk).isNotNull() & F.col('a.'+consistent).isNotNull()
            & ~F.col('a.'+consistent).eqNullSafe(F.col('b.'+consistent)))
        if contradiction.limit(1).count():
            raise ValueError('Relacionamento contraditório em '+entity+'. Gold bloqueada.')
        issues.append(linked.filter(F.col('a.'+fk).isNotNull() & F.col('b.'+pk).isNull()).select(
            F.col('a.env').alias('env'),F.lit(entity).alias('entity'),F.col('a.'+key).alias('entity_id'),
            F.lit(fk).alias('relation'),F.col('a.'+fk).alias('reference_id'),F.lit('orphan').alias('issue'),
            F.lit(run.run_id).alias('published_batch_id')))
    issue_frame=issues[0]
    for frame in issues[1:]: issue_frame=issue_frame.unionByName(frame)
    write_snapshot(spark,issue_frame,table(config,'silver','s_linkage_issues'))

    marker=spark.createDataFrame([(config['environment'],run.run_id,utcnow(),VERSION,'ready')],
        'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING, state STRING')
    write_snapshot(spark,marker,table(config,'silver','_publication'))
    print('Silver concluída: '+run.run_id)


def run_silver(spark, config):
    with DeltaStage(spark, config) as stage:
        return _run_silver(spark, config, stage)

# COMMAND ----------
config=read_config(dbutils)
run_silver(spark,config)
