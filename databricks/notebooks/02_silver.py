# Databricks notebook source
# MAGIC %md
# MAGIC Silver: UPPER e deduplicação; gravação de tabelas Delta comuns por snapshot.

# COMMAND ----------
import sys
import importlib
from pathlib import Path

for candidate in (Path.cwd().parent, Path.cwd()):
    if (candidate / "argos").is_dir():
        sys.path.insert(0, str(candidate))
        break
import argos.common as common_module

importlib.reload(common_module)
import argos.silver as silver_module

importlib.reload(silver_module)
import argos.gold as gold_module

importlib.reload(gold_module)
import argos.delta_layers as layers

importlib.reload(layers)

# COMMAND ----------
defaults = {
    "catalog": "main",
    "source_schema": "public",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "self_service",
}
for key, default in defaults.items():
    dbutils.widgets.text(key, default)
config = {key: dbutils.widgets.get(key) for key in defaults}
layers.run_silver(spark, config)
