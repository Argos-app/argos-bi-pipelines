# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — _context_user_property**
# MAGIC Relacionar proprietários e usuários vinculados às fazendas sem repetir vínculos.
# MAGIC Esta atividade grava somente `_context_user_property` e mantém a consulta original sem agregações.

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
GOLD_TABLE = '_context_user_property'
SILVER_TABLES = ('property', 'user_property')
GOLD_DEPENDENCIES = ()

statement = f"""
        SELECT user_id, id AS property_id FROM {silver('property')}
        UNION
        SELECT user_id, property_id FROM {silver('user_property')}
        WHERE active = true AND unlink_date IS NULL"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
