# Databricks notebook source
"""Valida todas as materialized views e confirma o lote para os dashboards."""
from pyspark.sql import functions as F
import sys
for key, default in [('project_root','/Workspace/Shared/argos-ai-bi'),('catalog','workspace'),
                     ('bronze_schema','bronze_ia'),('silver_schema','silver_ia'),('gold_schema','gold_ia')]:
    try: dbutils.widgets.get(key)
    except Exception: dbutils.widgets.text(key,default)
sys.path.insert(0, dbutils.widgets.get('project_root'))
from argos.common import DEFAULTS, table, write_snapshot, assert_unique, utcnow, VERSION
from argos.gold import GOLD_KEYS, GOLD_BASE
config=dict(DEFAULTS)
for key in ('catalog','bronze_schema','silver_schema','gold_schema'):
    config[key]=dbutils.widgets.get(key)
run=(spark.table(table(config,'bronze','_runs')).filter(F.col('env')=='argos')
     .orderBy(F.col('run_started_at').desc(),F.col('run_id').desc()).first())
if run is None: raise ValueError('Nenhum snapshot Bronze confirmado.')
gate=spark.table(table(config,'silver','_quality')).collect()
if len(gate)!=1 or gate[0].published_batch_id!=run.run_id or gate[0].errors!=0:
    raise ValueError('Silver não validou o último snapshot.')
for target,key in GOLD_KEYS.items():
    frame=spark.table(table(config,'gold',target))
    assert_unique(frame,['env',key],target)
    if frame.filter(~F.col('published_batch_id').eqNullSafe(F.lit(run.run_id))).limit(1).count():
        raise ValueError('Gold contém lote incorreto: '+target)
    if frame.count()!=spark.table(table(config,'silver',GOLD_BASE[target])).count():
        raise ValueError('Cardinalidade Gold incorreta: '+target)
# Verificar novamente para não confirmar um lote ultrapassado durante a validação.
last=(spark.table(table(config,'bronze','_runs')).filter(F.col('env')=='argos')
      .orderBy(F.col('run_started_at').desc(),F.col('run_id').desc()).first())
if last.run_id!=run.run_id: raise ValueError('Bronze mudou durante a validação.')
marker=spark.createDataFrame([('argos',run.run_id,utcnow(),VERSION)],
    'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING')
write_snapshot(spark,marker,table(config,'gold','_publication'))
print('Lote validado: '+run.run_id)
