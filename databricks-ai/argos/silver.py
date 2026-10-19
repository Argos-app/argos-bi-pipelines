"""Estado corrente, parsing tipado, quarentena e relacionamentos estruturais."""
from .common import VERSION, SOURCE_DEFINITIONS, table, sources, config_signature, ensure_schemas, write_snapshot, append_frame, assert_unique, utcnow
from .schemas import SCHEMAS, IDS, frame_schema
from .parsing import parse_record
from .bronze import latest_run

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

def parse_source(spark, config, alias, raw, run):
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StructType,StructField,StringType,ArrayType
    canonical='observations' if alias=='a2a_observations' else alias
    schema=frame_schema(spark,SCHEMAS[canonical])
    result_schema=StructType([StructField('record',schema),StructField('errors',ArrayType(StringType())),StructField('semantic_hash',StringType())])
    parser=F.udf(lambda text:parse_record(canonical,text),result_schema)
    parsed=raw.withColumn('p',parser('raw_json')).persist()
    try:
        count_bad=quarantine(spark,config,parsed.filter(F.size('p.errors')>0),run.run_id)
        if count_bad and config['fail_on_quarantine']:
            raise ValueError('Quarentena em '+alias+': '+str(count_bad)+' versões. Consulte silver.q_records; nenhuma Gold será publicada.')
        valid=parsed.filter(F.size('p.errors')==0)
        # Event IDs imutáveis e observações terminais não admitem conteúdo divergente.
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
            F.lit(VERSION).alias('transform_version'),F.lit(run.run_id).alias('published_batch_id')).localCheckpoint(eager=True)
    finally:
        parsed.unpersist()

def build_invocations(spark, config):
    from pyspark.sql import functions as F
    e=spark.table(table(config,'silver','s_execution_events'))
    assert_unique(e,['env','invocation_id','status'],'eventos por invocação/estado')
    z=e.filter("status IN ('succeeded','failed')")
    assert_unique(z,['env','invocation_id'],'término de invocação')
    a=e.filter("status='started'")
    keys=e.select('env','invocation_id').distinct()
    # Validar atributos entre início e término antes de coalesce.
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
            F.col('a.created_at').alias('started_at'),F.col('z.completed_at').alias('ended_at'),
            F.col('z.status').alias('terminal_status'),F.col('z.safe_error_code').alias('safe_error_code'),
            F.col('z.judge_decision').alias('judge_decision'),F.col('z.judge_parse_status').alias('judge_parse_status'),
            F.col('z.unsupported_claims').alias('unsupported_claims'),
            F.coalesce(F.col('z.published_batch_id'),F.col('a.published_batch_id')).alias('published_batch_id')))
    if result.filter('ended_at < started_at').limit(1).count():
        raise ValueError('Fim de invocação anterior ao início.')
    return result

def build_membership(spark, config):
    from pyspark.sql import functions as F
    i=spark.table(table(config,'silver','s_interactions'))
    m=spark.table(table(config,'silver','s_messages')).filter("role='user' AND kind='user_input'")
    candidates=m.groupBy('env','interaction_id').agg(F.count('*').alias('message_count'),
        F.first('source_interaction_id').alias('source_interaction_id'),F.first('answered_question_id').alias('answered_question_id'))
    # first só é usado quando message_count=1; ambíguos nunca são aceitos como link.
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
    resolved=resolved.localCheckpoint(eager=True)
    for _ in range(config['max_cycle_depth']):
        parents=resolved.select(F.col('env').alias('pe'),F.col('interaction_id').alias('pi'),F.col('root_interaction_id').alias('pr'))
        joined=resolved.join(parents,(resolved.env==parents.pe)&(resolved.valid_parent_id==parents.pi),'left')
        if not joined.filter('root_interaction_id IS NULL AND pr IS NOT NULL').limit(1).count():
            break
        nxt=joined.select('env','interaction_id','source_interaction_id','published_batch_id','valid_parent_id',
            F.coalesce('root_interaction_id','pr').alias('root_interaction_id')).localCheckpoint(eager=True)
        resolved.unpersist(); resolved=nxt
    # Qualquer bifurcação contamina a cadeia inteira, inclusive a raiz.
    branches=(resolved.filter('valid_parent_id IS NOT NULL').groupBy('env','valid_parent_id').count()
              .filter('count>1').select('env',F.col('valid_parent_id').alias('bid')))
    bad_roots=(resolved.join(branches,(resolved.env==branches.env)&(resolved.interaction_id==branches.bid),'inner')
        .select(resolved.env,resolved.root_interaction_id).distinct().withColumn('ambiguous',F.lit(True)))
    return (resolved.join(bad_roots,['env','root_interaction_id'],'left')
        .select('env','interaction_id','root_interaction_id','source_interaction_id','published_batch_id',
            F.when(F.col('root_interaction_id').isNull(),'unknown').when(F.col('ambiguous'),'ambiguous').otherwise('valid').alias('link_status')))

def run_silver(spark, config):
    from pyspark.sql import functions as F
    ensure_schemas(spark,config)
    spark.sql('CREATE TABLE IF NOT EXISTS '+table(config,'silver','q_records')+''' (
        env STRING, source_alias STRING, document_id STRING, bronze_version_key STRING,
        rule_ids ARRAY<STRING>, detected_at TIMESTAMP, parser_version STRING, published_batch_id STRING
    ) USING DELTA''')
    # Pré-criar antes das quatro tasks paralelas para evitar corrida de CREATE TABLE.
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
            frame=parse_source(spark,config,alias,committed_bronze(spark,config,alias,run),run)
        else:
            spec=dict(env='STRING',**SCHEMAS[canonical],source_alias='STRING',source_collection='STRING',source_database='STRING',
                bronze_version_key='STRING',source_updated_at='TIMESTAMP',transform_version='STRING',published_batch_id='STRING')
            frame=spark.createDataFrame([],frame_schema(spark,spec))
        if canonical=='observations':
            observation_frames.append(frame); continue
        assert_unique(frame,['env',IDS[canonical]],canonical)
        write_snapshot(spark,frame,table(config,'silver','s_'+canonical))
        frame.unpersist()
    obs=observation_frames[0].unionByName(observation_frames[1])
    assert_unique(obs,['env','observation_id'],'observations global API/A2A')
    write_snapshot(spark,obs,table(config,'silver','s_observations'))
    for frame in observation_frames: frame.unpersist()
    # Tipos/linhagem permanecem no fato, filhos têm posição explícita.
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
    membership=build_membership(spark,config)
    assert_unique(membership,['env','interaction_id'],'cycle_membership')
    write_snapshot(spark,membership,table(config,'silver','s_cycle_membership'))
    observations=spark.table(table(config,'silver','s_observations'))
    http=(observations.filter("source_alias='observations' AND operation_kind='http' AND operation_name='http.request' AND correlation_id IS NOT NULL")
        .select('env','correlation_id','observation_id',F.col('http.route').alias('request_route'),
            F.col('http.method').alias('request_method'),F.col('http.status_code').alias('request_status_code'),
            'started_at','ended_at','duration_ms','status','published_batch_id'))
    assert_unique(http,['env','correlation_id'],'http_requests')
    write_snapshot(spark,http,table(config,'silver','s_http_requests'))
    # Publicação Gold usa metadados de e-mail já minimizados na Silver.
    reports=spark.table(table(config,'silver','s_email_reports'))
    public=(reports.select('env','report_id','requested_by','published_batch_id',
        F.transform('recipients',lambda x:F.struct(x.recipient_id.alias('recipient_id'))).alias('recipients'),
        F.transform('attachments',lambda x:F.struct(x.filename.alias('filename'),x.content_type.alias('content_type'),
            x.sha256.alias('sha256'),x.size_bytes.alias('size_bytes'))).alias('attachments')))
    write_snapshot(spark,public,table(config,'silver','s_email_report_metadata'))
    # Órfãos são preservados e sinalizados; relações contraditórias bloqueiam a Gold.
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
    # Carimbo separado garante leitura do lote; não agrega indicadores.
    marker=spark.createDataFrame([(config['environment'],run.run_id,utcnow(),VERSION,'ready')],
        'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING, state STRING')
    write_snapshot(spark,marker,table(config,'silver','_publication'))
    print('Silver concluída: '+run.run_id)
