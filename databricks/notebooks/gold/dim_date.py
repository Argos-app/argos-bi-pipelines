# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — dim_date**
# MAGIC Disponibilizar o calendário para análises temporais.
# MAGIC Esta atividade grava somente `dim_date` e mantém a consulta original sem agregações.

# COMMAND ----------
import sys
import importlib
from pathlib import Path

# Também funciona quando o .py é importado como notebook, sem __file__.
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        sys.path.insert(0, str(candidate))
        break
import argos.common as common_module
importlib.reload(common_module)
import argos.delta_layers as layers
importlib.reload(layers)
from argos.common import source_name, table_name

# COMMAND ----------
defaults = {'catalog': 'main', 'source_schema': 'public', 'bronze_schema': 'bronze',
            'silver_schema': 'silver', 'gold_schema': 'self_service'}
for key, default in defaults.items():
    dbutils.widgets.text(key, default)
config = {key: dbutils.widgets.get(key) for key in defaults}


def silver(name):
    return table_name(config['catalog'], config['silver_schema'], source_name(config['source_schema'], name))


def gold(name):
    return table_name(config['catalog'], config['gold_schema'], name)

# COMMAND ----------
GOLD_TABLE = 'dim_date'
SILVER_TABLES = ('dim_date',)
GOLD_DEPENDENCIES = ()

statement = f"""SELECT e.*, CAST(NULL AS STRING) AS sf_user_id, CAST(NULL AS STRING) AS sf_user_name,
        CAST(NULL AS STRING) AS sf_property_id, CAST(NULL AS STRING) AS sf_property_name,
        CAST(NULL AS STRING) AS sf_property_city, CAST(NULL AS STRING) AS sf_property_state,
        CAST(NULL AS DECIMAL(10,2)) AS sf_area_hectares,
        CAST(NULL AS STRING) AS sf_owner_user_id, CAST(NULL AS STRING) AS sf_owner_user_name FROM {silver('dim_date')} e"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
