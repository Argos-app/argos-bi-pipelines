"""Massa sintética para validar a DAG sem conexão com Mongo ou LLM."""
import json
from datetime import datetime,timedelta,timezone
from uuid import uuid4
from .common import VERSION, sources, config_signature, ensure_schemas, table, append_frame, utcnow
from .schemas import BRONZE_DDL,STATE_DDL,RUN_DDL
from .bronze import bronze_record
from .silver import run_silver
from .gold import run_gold,validate_pipeline,GOLD_KEYS

def fixture_documents():
    from bson.decimal128 import Decimal128
    ids={key:str(uuid4()) for key in ('session','root','answer','user','farm','question','report','operation','artifact','preparation','authorization','invocation','correlation','graph','agent1','agent2')}
    d=datetime(2026,10,19,12,tzinfo=timezone.utc)
    expires=d+timedelta(days=1)
    result1={'schema_version':'1','status':'completed','degradation_reasons':[],
        'session_id':ids['session'],'interaction_id':ids['root'],'public_request_id':str(uuid4()),
        'response':{'kind':'question','question_id':ids['question'],'input_type':'free_text','prompt':'Fixture',
                    'reason_code':'fixture','options':[],'expires_at':expires.isoformat()}}
    result2=dict(result1,interaction_id=ids['answer'],response={'kind':'answer','text':'Resposta sintética'})
    interaction=lambda key,kind,result:dict(_id=ids[key],session_id=ids['session'],user_id=ids['user'],
        deduplication_key=str(uuid4()),kind=kind,sanitized_input='Fixture',input_hash='fixture',status='completed',
        created_at=d,updated_at=d+timedelta(seconds=12),version=3,result_json=json.dumps(result))
    messages=[]
    for seq,key,payload in ((1,'root',{'kind':'message','message':'Fixture'}),
                            (2,'answer',{'kind':'answer','interaction_id':ids['root'],'question_id':ids['question'],'answer':{'kind':'text','text':'Fixture'}})):
        messages.append(dict(_id=str(uuid4()),session_id=ids['session'],interaction_id=ids[key],user_id=ids['user'],
            sequence=seq,role='user',kind='user_input',sanitized_content='Fixture',created_at=d,user_message_number=seq,payload_json=json.dumps(payload)))
    events=[]
    for status in ('started','succeeded'):
        events.append(dict(_id=str(uuid4()),invocation_id=ids['invocation'],session_id=ids['session'],interaction_id=ids['root'],
            user_id=ids['user'],target='agent',target_name='judge',status=status,attempt=1,
            created_at=d if status=='started' else d+timedelta(seconds=2),
            completed_at=None if status=='started' else d+timedelta(seconds=2),
            sanitized_output=None if status=='started' else json.dumps({'decision':'approved','reason':'Fixture','unsupported_claims':[]}),messages=[]))
    obs=[]
    def span(oid,kind,name,start,end,parent=None,agent=None):
        return dict(_id=oid,schema_version='2',service='main_api',environment='fixture',operation_kind=kind,
            operation_name=name,observation_level='sdk_invocation' if kind in ('agent','model') else 'local_operation',
            execution_mode='mock',status='succeeded',correlation_id=ids['correlation'],session_id=ids['session'],
            interaction_id=ids['root'],parent_observation_id=parent,agent=agent,
            started_at=d+timedelta(seconds=start),ended_at=d+timedelta(seconds=end),
            updated_at=d+timedelta(seconds=end),duration_ms=float((end-start)*1000))
    obs.append(span(ids['graph'],'graph','graph.execute',0,10))
    obs.append(span(ids['agent1'],'agent','agent.farm',1,5,ids['graph'],'farm'))
    obs.append(span(ids['agent2'],'agent','agent.doctor',3,7,ids['graph'],'doctor'))
    model=span(str(uuid4()),'model','model.invoke',2,3,ids['agent1'],'farm')
    model.update(provider='fixture',model_requested='fixture-model',usage={'availability':'returned','source':'provider','input_tokens':100,'output_tokens':20,'total_tokens':120,'details':{}},
        billing={'reported_cost':Decimal128('0.00012345678901234567'),'currency':'USD','cost_source':'provider'},
        resources=[{'name':'fixture','quantity':Decimal128('1.0000000000'),'unit':'request','source':'application'}])
    obs.append(model)
    envelope={'report':{'schema_version':'lot-report.v1','requested_lot':{
        'lot_id':str(uuid4()),'lot_code':'FIXTURE','lot_category':'ENGORDA','lot_active':True,'lot_is_exception':False,
        'lot_head_count':1,'lot_open_date':'2026-10-01','fit_for_slaughter':False,'herd_id':str(uuid4()),
        'herd_name':'Fixture','herd_breed':'Fixture','property_id':ids['farm'],'property_name':'Fixture'},
        'applications':[],'exception_animals':[],'queried_at':d.isoformat()},'principal_user_id':ids['user'],
        'authorized_property_ids':[ids['farm']],'source_service':'mcp_data','valid_until':expires.isoformat(),
        'payload_hash':'fixture','signature':'fixture','signature_key_id':'fixture'}
    prepared={'report_id':ids['report'],'status':'prepared','requested_by':ids['user'],'recipients':[],
        'attachments':[],'created_at':d.isoformat(),'expires_at':expires.isoformat()}
    docs={
      'sessions':[dict(_id=ids['session'],user_id=ids['user'],farm_ids=[ids['farm']],created_at=d,updated_at=d,
                       user_message_count=2,next_message_sequence=3,version=1,lease_fencing_token=0)],
      'interactions':[interaction('root','message',result1),interaction('answer','answer',result2)],
      'messages':messages,'execution_events':events,'observations':obs,'a2a_observations':[],
      'session_summaries':[dict(_id=ids['session'],revision=1,through_sequence=2,through_user_message_number=2,
        activity='fixture',important_facts=[],decisions=[],limitations=[],summary_text='fixture',token_count=2,updated_at=d)],
      'user_memory':[dict(_id=ids['user'],tone='fixture',revision=1,updated_at=d)],
      'report_artifacts':[dict(_id=ids['artifact'],principal_user_id=ids['user'],session_id=ids['session'],interaction_id=ids['root'],task_id='fixture',created_at=d,expires_at=expires,purge_at=expires,envelope=envelope)],
      'email_preparations':[dict(_id=ids['preparation'],principal_user_id=ids['user'],session_id=ids['session'],interaction_id=ids['root'],report_artifact_ref=ids['artifact'],request_fingerprint='fixture',created_at=d,expires_at=expires,purge_at=expires,prepared_report=prepared)],
      'pending_recipient_resolutions':[dict(_id=ids['question'],principal_user_id=ids['user'],session_id=ids['session'],interaction_id=ids['root'],report_artifact_ref=ids['artifact'],input_type='free_text',reason_code='fixture',question_goal='fixture',options=[],expires_at=expires,purge_at=expires)],
      'email_authorizations':[dict(_id=ids['authorization'],principal_user_id=ids['user'],session_id=ids['session'],source_interaction_id=ids['root'],preparation_reference=ids['preparation'],report_id=ids['report'],origin='explicit_request',idempotency_key='fixture',authorized_at=d,expires_at=expires,purge_at=expires)],
      'email_operation_references':[dict(_id=ids['authorization'],principal_user_id=ids['user'],session_id=ids['session'],operation_id=ids['operation'],recorded_at=d,purge_at=expires)],
      'email_reports':[dict(_id=ids['report'],requested_by=ids['user'],recipients=[],attachments=[],state='prepared',created_at=d,expires_at=expires)],
      'email_dispatches':[dict(_id=ids['operation'],report_id=ids['report'],requested_by=ids['user'],idempotency_key='fixture',state='accepted',created_at=d,updated_at=d)],
    }
    return docs,ids

def run_fixture(spark, base_config):
    from pyspark.sql import functions as F
    from decimal import Decimal
    suffix=uuid4().hex[:8]
    config=dict(base_config,environment='fixture_'+suffix,collections_json='{}',mongo_database='fixture',mongo_a2a_database='')
    for layer in ('bronze','silver','gold'):
        config[layer+'_schema']=base_config[layer+'_schema']+'_fixture_'+suffix
    ensure_schemas(spark,config)
    docs,ids=fixture_documents(); run_id=str(uuid4()); started=utcnow(); signature=config_signature(config)
    for source in sources(config):
        target=table(config,'bronze','b_'+source['alias'])
        spark.sql('CREATE TABLE '+target+' ('+BRONZE_DDL+') USING DELTA')
        rows=[bronze_record(doc,source,config,run_id,started) for doc in docs[source['alias']]]
        if rows: append_frame(spark,spark.createDataFrame(rows,BRONZE_DDL),target)
        state=dict(env=config['environment'],source_alias=source['alias'],run_id=run_id,run_started_at=started,
            completed_at=utcnow(),is_snapshot=True,watermark=None,documents=len(rows),source_signature=signature)
        append_frame(spark,spark.createDataFrame([state],STATE_DDL),table(config,'bronze','_ingestion_state'))
    run=dict(env=config['environment'],run_id=run_id,run_started_at=started,completed_at=utcnow(),source_signature=signature,
        source_aliases=[s['alias'] for s in sources(config)],extractor_version=VERSION)
    append_frame(spark,spark.createDataFrame([run],RUN_DDL),table(config,'bronze','_runs'))
    run_silver(spark,config)
    for name in GOLD_KEYS: run_gold(spark,config,name)
    validate_pipeline(spark,config)
    assert spark.table(table(config,'gold','g_interactions')).count()==2
    assert spark.table(table(config,'gold','g_observations')).count()==4
    assert spark.table(table(config,'gold','g_invocations')).count()==1
    inv=spark.table(table(config,'gold','g_invocations')).first()
    assert (inv.ended_at-inv.started_at).total_seconds()==2
    membership=spark.table(table(config,'silver','s_cycle_membership')).collect()
    assert all(x.root_interaction_id==ids['root'] and x.link_status=='valid' for x in membership)
    cost=spark.table(table(config,'gold','g_observations')).agg(F.sum('billing.reported_cost')).first()[0]
    assert cost==Decimal('0.00012345678901234567')
    assert spark.table(table(config,'silver','s_observation_resources')).count()==1
    filtered=spark.table(table(config,'gold','g_interactions')).filter(F.array_contains('farm_ids',ids['farm']))
    assert filtered.count()==2
    print('Fixture aprovada; 3 schemas de teste preservados: '+', '.join(config[x+'_schema'] for x in ('bronze','silver','gold')))
    return config
