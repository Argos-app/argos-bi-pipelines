# Databricks notebook source
# MAGIC %pip install pymongo==4.19.0

# COMMAND ----------
import sys
from pathlib import Path
try:
    project_root = dbutils.widgets.get('project_root')
except Exception:
    dbutils.widgets.text('project_root', '/Workspace/Shared/argos-ai-bi')
    project_root = dbutils.widgets.get('project_root')
root = Path(project_root)
if not root.is_absolute() or not (root / 'argos').is_dir():
    raise RuntimeError('Configure project_root com a raiz absoluta do projeto no Workspace ou use os notebooks autocontidos.')
sys.path.insert(0, str(root))
from argos.common import read_config
from argos.bronze import ingest

# COMMAND ----------
# Snapshot JSONL completo em jsonl_path (/Volumes).
config=read_config(dbutils)
ingest(spark,config)
