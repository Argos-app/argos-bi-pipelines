"""Uma consulta por tabela Gold: apenas SELECT e LEFT JOIN sobre Silver."""
from .common import VERSION, table, ensure_schemas, write_snapshot, append_frame, assert_unique, assert_delta, utcnow

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
    # O schema Silver não pode ser alterado enquanto as tasks Gold estão executando.
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
    # Marcador de detalhe: nenhuma métrica de dashboard é materializada.
    marker=spark.createDataFrame([(config['environment'],batch,utcnow(),VERSION)],
        'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING')
    write_snapshot(spark,marker,table(config,'gold','_publication'))
    print('Validação aprovada. Lote disponível para dashboards: '+batch)
