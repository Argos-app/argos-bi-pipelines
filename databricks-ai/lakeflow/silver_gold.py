# Databricks notebook source
# Databricks notebook source
"""Lakeflow BI de IA. As funções declaradas somente retornam DataFrames."""
import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

from pathlib import Path
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        root = candidate
        break
else:
    root = Path(spark.conf.get('argos_ai.project_root', '/Workspace/Shared/argos-ai-bi'))
    if not root.is_absolute() or not (root / 'argos').is_dir():
        raise RuntimeError('Pasta argos não encontrada. Preserve argos/ e lakeflow/ na mesma raiz do projeto.')
sys.path.insert(0, str(root))
from argos.common import VERSION, SOURCE_DEFINITIONS, DEFAULTS, sources, config_signature, ident
from argos.schemas import SCHEMAS, IDS, ddl
from argos.parsing import parse_record
from argos.gold import gold_queries

config = dict(DEFAULTS)
for key in ('catalog', 'bronze_schema', 'silver_schema', 'gold_schema', 'source_database',
            'source_a2a_database', 'collections_json', 'mongo_source_id'):
    config[key] = spark.conf.get('argos_ai.' + key, config[key])
for key in ('catalog', 'bronze_schema', 'silver_schema', 'gold_schema'):
    ident(config[key])
active = {item['alias'] for item in sources(config)}
if active != {item[0] for item in SOURCE_DEFINITIONS}:
    raise ValueError('Lakeflow exige os 15 aliases habilitados; forneça arquivos vazios quando necessário.')
expected_signature = config_signature(config)
max_depth = int(spark.conf.get('argos_ai.max_cycle_depth', '100'))
if not 1 <= max_depth <= 1000:
    raise ValueError('max_cycle_depth deve estar entre 1 e 1000.')
if spark.conf.get('argos_ai.fail_on_quarantine', 'true').lower() != 'true':
    raise ValueError('Lakeflow exige fail_on_quarantine=true.')

def name(layer, obj):
    return '.'.join(ident(x) for x in (config['catalog'], config[layer + '_schema'], obj))

def read(layer, obj):
    return spark.read.table(name(layer, obj))

def register(layer, obj, function, expectation=None):
    function.__name__ = 'dataset_' + obj
    if expectation:
        function = dp.expect_or_fail('integridade', expectation)(function)
    dp.materialized_view(name='.'.join((config['catalog'], config[layer + '_schema'], obj)))(function)

def latest():
    runs = read('bronze', '_runs').filter(F.col('env') == 'argos')
    return (runs.withColumn('_rn', F.row_number().over(Window.orderBy(F.col('run_started_at').desc(), F.col('run_id').desc())))
            .filter('_rn=1').drop('_rn'))

# Apenas o snapshot global confirmado: uma carga falha jamais vira entrada da Silver.
def parsed_source(alias):
    canonical = 'observations' if alias == 'a2a_observations' else alias
    schema = 'STRUCT<record:' + ddl(SCHEMAS[canonical]) + ',errors:ARRAY<STRING>,semantic_hash:STRING>'
    parser = F.udf(lambda raw: parse_record(canonical, raw), schema)
    frame = (read('bronze', 'b_' + alias).alias('b').join(latest().alias('r'),
             (F.col('b.env') == F.col('r.env')) & (F.col('b.run_id') == F.col('r.run_id')), 'inner')
             .select('b.*').withColumn('p', parser('raw_json')))
    return frame

for alias, _, _, _ in SOURCE_DEFINITIONS:
    if alias in active:
        def make_parsed(alias=alias):
            return parsed_source(alias)
        register('silver', '_parsed_' + alias, make_parsed)

# Um dataset de quarentena consultável, sem incluir payloads brutos.
def quarantine():
    result = None
    for alias in sorted(active):
        frame = read('silver', '_parsed_' + alias)
        bad = frame.filter(F.size('p.errors') > 0).select(
            'env', 'source_alias', 'document_id', F.col('version_key').alias('bronze_version_key'),
            F.col('p.errors').alias('rule_ids'), F.col('run_id').alias('published_batch_id'))
        canonical = 'observations' if alias == 'a2a_observations' else alias
        if canonical in ('observations', 'execution_events'):
            good = frame.filter(F.size('p.errors') == 0)
            terminal = good.filter(F.col('p.record.status') != 'started') if canonical == 'observations' else good
            conflicts = (terminal.groupBy('env', 'document_id').agg(F.countDistinct('p.semantic_hash').alias('variants'))
                         .filter('variants > 1').select('env', 'document_id'))
            conflict = (good.join(conflicts, ['env', 'document_id'], 'inner').select(
                'env', 'source_alias', 'document_id', F.col('version_key').alias('bronze_version_key'),
                F.array(F.lit('identity:conflicting_content')).alias('rule_ids'), F.col('run_id').alias('published_batch_id')))
            bad = bad.unionByName(conflict)
        result = bad if result is None else result.unionByName(bad)
    return result
register('silver', 'q_records', quarantine)

def current(alias):
    canonical = 'observations' if alias == 'a2a_observations' else alias
    if alias not in active:
        # Preserva schema para origens opcionalmente desabilitadas sem consultar tabela inexistente.
        template = next(a for a in active if ('observations' if a == 'a2a_observations' else a) == canonical)
        return current(template).filter(F.lit(False))
    frame = read('silver', '_parsed_' + alias).filter(F.size('p.errors') == 0)
    bad = read('silver', 'q_records').filter(F.col('source_alias') == alias).select('env', 'document_id').distinct()
    frame = frame.join(bad, ['env', 'document_id'], 'left_anti')
    order = []
    if canonical == 'observations':
        order.append(F.when(F.col('p.record.status') == 'started', 0).otherwise(1).desc())
    order.extend([F.col('source_version').desc_nulls_last(), F.col('source_updated_at').desc_nulls_last(),
                  F.col('run_started_at').desc(), F.col('version_key').desc()])
    return (frame.withColumn('_rn', F.row_number().over(Window.partitionBy('env', 'document_id').orderBy(*order)))
            .filter('_rn=1').select('env', 'p.record.*', 'source_alias', 'source_collection', 'source_database',
                   F.col('version_key').alias('bronze_version_key'), 'source_updated_at',
                   F.lit(VERSION).alias('transform_version'), F.col('run_id').alias('published_batch_id')))

for alias, _, _, _ in SOURCE_DEFINITIONS:
    if alias in ('observations', 'a2a_observations') or alias not in active:
        continue
    def make_current(alias=alias):
        return current(alias)
    register('silver', 's_' + alias, make_current)

def observations():
    aliases = [a for a in ('observations', 'a2a_observations') if a in active]
    if not aliases:
        raise ValueError('Ao menos uma origem observations deve estar habilitada.')
    result = current(aliases[0])
    for alias in aliases[1:]:
        result = result.unionByName(current(alias))
    return result
register('silver', 's_observations', observations)

def invocations():
    e = read('silver', 's_execution_events')
    a, z = e.filter("status='started'").alias('a'), e.filter("status IN ('succeeded','failed')").alias('z')
    return (e.select('env','invocation_id').distinct().join(a, ['env','invocation_id'], 'left')
        .join(z, ['env','invocation_id'], 'left').select('env','invocation_id',
        *[F.coalesce(F.col('a.'+k),F.col('z.'+k)).alias(k) for k in ('interaction_id','session_id','target','target_name','attempt')],
        F.col('a.event_id').alias('start_event_id'),F.col('z.event_id').alias('end_event_id'),
        F.coalesce(F.col('a.created_at'),F.col('z.created_at')).alias('started_at'), F.col('z.completed_at').alias('ended_at'),
        F.col('z.status').alias('terminal_status'), F.col('z.safe_error_code').alias('safe_error_code'),
        F.col('z.judge_decision').alias('judge_decision'), F.col('z.judge_parse_status').alias('judge_parse_status'),
        F.col('z.unsupported_claims').alias('unsupported_claims'),
        F.coalesce(F.col('z.published_batch_id'),F.col('a.published_batch_id')).alias('published_batch_id')))
register('silver', 's_invocations', invocations)

def membership():
    # A recursão tem profundidade limitada; caminhos maiores ficam unknown, assim como antes.
    i, m = name('silver','s_interactions'), name('silver','s_messages')
    return spark.sql(f'''WITH RECURSIVE
      candidates AS (SELECT env,interaction_id,count(*) n,first(source_interaction_id) parent,
                     first(answered_question_id) question FROM {m}
                     WHERE role='user' AND kind='user_input' GROUP BY env,interaction_id),
      links AS (SELECT i.env,i.interaction_id,i.interaction_kind,i.published_batch_id,c.parent AS source_interaction_id,
                CASE WHEN i.interaction_kind='answer' AND c.n=1 AND p.interaction_id IS NOT NULL
                  AND i.session_id=p.session_id AND i.user_id=p.user_id AND p.response_kind='question'
                  AND c.question=p.question_id THEN p.interaction_id END AS valid_parent_id
                FROM {i} i LEFT JOIN candidates c ON i.env=c.env AND i.interaction_id=c.interaction_id
                LEFT JOIN {i} p ON i.env=p.env AND c.parent=p.interaction_id),
      walk(env,interaction_id,root_interaction_id,depth) MAX RECURSION LEVEL {max_depth + 1} AS (
        SELECT env,interaction_id,interaction_id,0 FROM links WHERE interaction_kind='message'
        UNION ALL
        SELECT l.env,l.interaction_id,w.root_interaction_id,w.depth+1 FROM links l JOIN walk w
          ON l.env=w.env AND l.valid_parent_id=w.interaction_id WHERE w.depth < {max_depth}),
      branches AS (SELECT env,valid_parent_id FROM links WHERE valid_parent_id IS NOT NULL
                   GROUP BY env,valid_parent_id HAVING count(*) > 1),
      ambiguous AS (SELECT DISTINCT w.env,w.root_interaction_id FROM walk w JOIN branches b
                    ON w.env=b.env AND w.interaction_id=b.valid_parent_id)
      SELECT l.env,l.interaction_id,w.root_interaction_id,l.source_interaction_id,l.published_batch_id,
        CASE WHEN w.root_interaction_id IS NULL THEN 'unknown' WHEN a.root_interaction_id IS NOT NULL
             THEN 'ambiguous' ELSE 'valid' END AS link_status
      FROM links l LEFT JOIN walk w ON l.env=w.env AND l.interaction_id=w.interaction_id
      LEFT JOIN ambiguous a ON w.env=a.env AND w.root_interaction_id=a.root_interaction_id''')
register('silver','s_cycle_membership',membership)

def http_requests():
    return (read('silver','s_observations').filter("source_alias='observations' AND operation_kind='http' AND operation_name='http.request' AND correlation_id IS NOT NULL")
        .select('env','correlation_id','observation_id',F.col('http.route').alias('request_route'),
          F.col('http.method').alias('request_method'),F.col('http.status_code').alias('request_status_code'),
          'started_at','ended_at','duration_ms','status','published_batch_id'))
register('silver','s_http_requests',http_requests)

def report_metadata():
    return read('silver','s_email_reports').select('env','report_id','requested_by','published_batch_id',
        F.transform('recipients',lambda x:F.struct(x.recipient_id.alias('recipient_id'))).alias('recipients'),
        F.transform('attachments',lambda x:F.struct(x.filename.alias('filename'),x.content_type.alias('content_type'),
          x.sha256.alias('sha256'),x.size_bytes.alias('size_bytes'))).alias('attachments'))
register('silver','s_email_report_metadata',report_metadata)

def event_messages():
    return (read('silver','s_execution_events').select('env','event_id',F.posexplode('messages').alias('message_pos','message'))
            .select('env','event_id','message_pos','message.*'))
register('silver','s_event_messages',event_messages)

def observation_resources():
    return (read('silver','s_observations').select('env','observation_id',F.posexplode('resources').alias('resource_pos','resource'))
            .select('env','observation_id','resource_pos','resource.*'))
register('silver','s_observation_resources',observation_resources)

def linkage_issues():
    issues=[]
    for entity,key,fk,right,pk in (
        ('s_interactions','interaction_id','session_id','s_sessions','session_id'),
        ('s_observations','observation_id','interaction_id','s_interactions','interaction_id'),
        ('s_execution_events','event_id','interaction_id','s_interactions','interaction_id'),
        ('s_messages','message_id','interaction_id','s_interactions','interaction_id')):
        a,b=read('silver',entity).alias('a'),read('silver',right).alias('b')
        joined=a.join(b,(F.col('a.env')==F.col('b.env')) & (F.col('a.'+fk)==F.col('b.'+pk)),'left')
        issues.append(joined.filter(F.col('a.'+fk).isNotNull() & F.col('b.'+pk).isNull()).select(
            F.col('a.env').alias('env'),F.lit(entity).alias('entity'),F.col('a.'+key).alias('entity_id'),
            F.lit(fk).alias('relation'),F.col('a.'+fk).alias('reference_id'),F.lit('orphan').alias('issue'),
            F.col('a.published_batch_id').alias('published_batch_id')))
    result=issues[0]
    for frame in issues[1:]: result=result.unionByName(frame)
    return result
register('silver','s_linkage_issues',linkage_issues)


# Cada verificação gera uma contagem pequena; o gate consolida essas contagens.
def count_issues(*frames):
    result = frames[0].select(F.lit(1).alias('bad'))
    for frame in frames[1:]:
        result = result.unionByName(frame.select(F.lit(1).alias('bad')))
    return result.agg(F.count('*').alias('errors'))

def quality_states():
    run = latest()
    states = read('bronze','_ingestion_state').alias('s').join(run.alias('r'),
        (F.col('s.env')==F.col('r.env')) & (F.col('s.run_id')==F.col('r.run_id')), 'inner')
    valid = states.filter(F.col('s.is_snapshot') & (F.col('s.source_signature')==expected_signature))
    counts = valid.groupBy(F.col('s.source_alias').alias('alias')).count()
    # Inclui também origens sem estado, sem criar uma união por origem.
    expected = spark.range(1).select(F.explode(F.array(*[F.lit(alias) for alias in sorted(active)])).alias('alias'))
    missing = expected.join(counts, 'alias', 'left').filter(F.coalesce(F.col('count'), F.lit(0)) != 1)
    return count_issues(missing, run.filter(F.col('source_signature') != expected_signature), read('silver','q_records'))

register('silver', '_quality_states', quality_states)

def quality_unique(obj, keys):
    frame = read('silver',obj)
    null = None
    for key in keys:
        condition = F.col(key).isNull()
        null = condition if null is None else null | condition
    return count_issues(frame.filter(null), frame.groupBy(*keys).count().filter('count > 1'))

unique_checks = [(alias, 's_'+alias, ['env', IDS[alias]])
    for alias, _, _, _ in SOURCE_DEFINITIONS
    if alias in active and alias not in ('observations','a2a_observations')]
unique_checks += [
    ('observations', 's_observations', ['env','observation_id']),
    ('event_invocation_status', 's_execution_events', ['env','invocation_id','status']),
    ('message_sequence', 's_messages', ['env','session_id','sequence']),
    ('cycle', 's_cycle_membership', ['env','interaction_id']),
    ('http', 's_http_requests', ['env','correlation_id']),
    ('invocations', 's_invocations', ['env','invocation_id']),
]
quality_parts = ['_quality_states']
for label, obj, keys in unique_checks:
    def make_unique(obj=obj, keys=keys):
        return quality_unique(obj, keys)
    part = '_quality_unique_' + label
    register('silver', part, make_unique)
    quality_parts.append(part)

def quality_events():
    events=read('silver','s_execution_events')
    terminal=events.filter("status IN ('succeeded','failed')").groupBy('env','invocation_id').count().filter('count>1')
    started=events.filter("status='started'").alias('a')
    ended=events.filter("status IN ('succeeded','failed')").alias('z')
    paired=started.join(ended,['env','invocation_id'],'inner')
    mismatch=None
    for field in ('session_id','interaction_id','target','target_name','attempt'):
        expr=~F.col('a.'+field).eqNullSafe(F.col('z.'+field))
        mismatch=expr if mismatch is None else mismatch | expr
    return count_issues(terminal, paired.filter(mismatch), read('silver','s_invocations').filter('ended_at < started_at'))

register('silver', '_quality_events', quality_events)
quality_parts.append('_quality_events')

for entity,fk,right,pk,consistent in (
    ('s_interactions','session_id','s_sessions','session_id','user_id'),
    ('s_observations','interaction_id','s_interactions','interaction_id','session_id'),
    ('s_execution_events','interaction_id','s_interactions','interaction_id','session_id'),
    ('s_messages','interaction_id','s_interactions','interaction_id','session_id')):
    def make_consistency(entity=entity, fk=fk, right=right, pk=pk, consistent=consistent):
        a,b=read('silver',entity).alias('a'),read('silver',right).alias('b')
        joined=a.join(b,(F.col('a.env')==F.col('b.env')) & (F.col('a.'+fk)==F.col('b.'+pk)),'inner')
        return count_issues(joined.filter(F.col('a.'+consistent).isNotNull() & ~F.col('a.'+consistent).eqNullSafe(F.col('b.'+consistent))))
    part = '_quality_consistency_' + entity[2:]
    register('silver', part, make_consistency)
    quality_parts.append(part)

def quality_group(parts):
    result = read('silver', parts[0])
    for part in parts[1:]:
        result = result.unionByName(read('silver', part))
    return result.agg(F.coalesce(F.sum('errors'), F.lit(0)).alias('errors'))

# Agrupar em camadas limita a profundidade mesmo com muitas verificações.
while len(quality_parts) > 5:
    groups = []
    for index in range(0, len(quality_parts), 5):
        parts = quality_parts[index:index+5]
        group = '_quality_group_' + str(len(quality_parts)) + '_' + str(index // 5)
        def make_group(parts=parts):
            return quality_group(parts)
        register('silver', group, make_group)
        groups.append(group)
    quality_parts = groups

def quality():
    return latest().select(F.col('run_id').alias('published_batch_id')).crossJoin(quality_group(quality_parts))
register('silver','_quality',quality,'errors = 0')

for gold_name, statement in gold_queries(config).items():
    def make_gold(statement=statement):
        frame=spark.sql(statement)
        gate=read('silver','_quality').select(F.col('published_batch_id').alias('_quality_batch'))
        return frame.join(gate,frame.published_batch_id==gate._quality_batch,'inner').drop('_quality_batch')
    register('gold',gold_name,make_gold)
