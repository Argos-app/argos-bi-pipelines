# Databricks notebook source
# MAGIC %pip install pymongo==4.19.0

# COMMAND ----------
import sys
from pathlib import Path
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        sys.path.insert(0,str(candidate))
        break
else:
    raise RuntimeError('Preserve argos/ e notebooks/ na mesma raiz OU use copiar_colar/.')
from argos.common import read_config
from argos.fixture import run_fixture

# COMMAND ----------
config=read_config(dbutils)
fixture_config=run_fixture(spark,config)
