# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "pymongo==4.19.0",
# ]
# ///
dbutils.library.restartPython()


# COMMAND ----------

# MAGIC %pip install pymongo==4.19.0


# COMMAND ----------

import sys
from pathlib import Path

# Mesmo padrão do relacional: procurar a raiz do projeto a partir do notebook.
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        root = candidate
        break
else:
    try:
        root = Path(dbutils.widgets.get('project_root'))
    except Exception:
        root = None
    if root is None or not root.is_absolute() or not (root / 'argos').is_dir():
        raise RuntimeError('Pasta argos não encontrada. Preserve argos/ e notebooks/ na mesma raiz do projeto.')
sys.path.insert(0, str(root))
from argos.common import read_config
from argos.bronze import ingest


# COMMAND ----------

mongo_uri = 'PREENCHA_URI_MONGODB'

config=read_config(dbutils)
config.update(mongo_uri=mongo_uri)
ingest(spark,config)


# COMMAND ----------

print("Database configurado:", config["source_database"])