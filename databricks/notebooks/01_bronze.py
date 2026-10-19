# Databricks notebook source
# MAGIC %md
# MAGIC Bronze: SELECT * via JDBC; versões e snapshots em Delta append.

# COMMAND ----------
import sys
import importlib
from pathlib import Path

for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        sys.path.insert(0, str(candidate))
        break
else:
    raise RuntimeError('Pasta argos não encontrada. Preserve argos/ e notebooks/ na raiz do projeto.')
import argos.common as common_module
import argos.bronze as bronze_module

importlib.reload(common_module)
importlib.reload(bronze_module)
ingest = bronze_module.ingest

# COMMAND ----------
# Preencha a senha diretamente nesta célula antes de executar.
# Credenciais não são widgets nem parâmetros do Job.
jdbc_user = 'avnadmin'
jdbc_password = 'PREENCHA_A_SENHA_DO_BANCO'

defaults = {
    'jdbc_url': (
        'jdbc:postgresql://banco-de-dados-institutojef-6ce7.c.aivencloud.com:24330/'
        'defaultdb?sslmode=require'
    ),
    'catalog': 'main', 'bronze_schema': 'bronze', 'silver_schema': 'silver',
    'gold_schema': 'self_service', 'source_schema': 'public',
    'lookback_hours': '24', 'force_snapshot': 'false',
}
for key, default in defaults.items():
    dbutils.widgets.text(key, default)

config = {key: dbutils.widgets.get(key) for key in defaults}
config.update(jdbc_user=jdbc_user, jdbc_password=jdbc_password)
ingest(spark, config)
