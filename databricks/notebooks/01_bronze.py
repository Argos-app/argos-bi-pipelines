# Databricks notebook source
# MAGIC %md
# MAGIC Bronze: SELECT * via JDBC; versões e snapshots em Delta append.

# COMMAND ----------
import sys
import importlib
import inspect
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
import argos.common as common_module
import argos.bronze as bronze_module

# Recarrega módulos após atualizar os arquivos no Workspace.
# Atualize common.py e bronze.py junto com este notebook.
importlib.reload(common_module)
importlib.reload(bronze_module)
ingest = bronze_module.ingest
if tuple(inspect.signature(ingest).parameters) != ('spark', 'config'):
    raise RuntimeError(
        f'Versão antiga de ingest carregada de {bronze_module.__file__}. '
        'Atualize argos/bronze.py para def ingest(spark, config) e execute esta célula novamente.'
    )

# COMMAND ----------
# Preencha a senha diretamente nesta célula antes de executar/deployar.
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
