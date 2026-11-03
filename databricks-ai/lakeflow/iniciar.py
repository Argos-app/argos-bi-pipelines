# Databricks notebook source
# Invalida a publicação antes da Bronze e de qualquer atualização da pipeline.
import sys
for key, default in [('project_root','/Workspace/Shared/argos-ai-bi'),('catalog','workspace'),('gold_schema','gold_ia')]:
    try: dbutils.widgets.get(key)
    except Exception: dbutils.widgets.text(key,default)
sys.path.insert(0,dbutils.widgets.get('project_root'))
from argos.common import DEFAULTS, ident, table, write_snapshot
config=dict(DEFAULTS)
for key in ('catalog','gold_schema'): config[key]=dbutils.widgets.get(key)
spark.sql('CREATE SCHEMA IF NOT EXISTS '+ident(config['catalog'])+'.'+ident(config['gold_schema']))
empty=spark.createDataFrame([], 'env STRING, published_batch_id STRING, completed_at TIMESTAMP, transform_version STRING')
write_snapshot(spark,empty,table(config,'gold','_publication'))
