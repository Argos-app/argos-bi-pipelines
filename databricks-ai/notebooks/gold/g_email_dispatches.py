# Databricks notebook source
import sys
from pathlib import Path
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        sys.path.insert(0,str(candidate))
        break
else:
    raise RuntimeError('Preserve argos/ e notebooks/ na mesma raiz OU use copiar_colar/.')
from argos.common import read_config
from argos.delta_layers import run_gold

# COMMAND ----------
config=read_config(dbutils)
run_gold(spark,config,'g_email_dispatches')
