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
