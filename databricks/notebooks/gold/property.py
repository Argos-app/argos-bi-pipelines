# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — property**
# MAGIC Disponibilizar o cadastro das fazendas e seus proprietários.
# MAGIC Esta atividade grava somente `property` e mantém a consulta original sem agregações.

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
GOLD_TABLE = 'property'
SILVER_TABLES = ('app_user', 'property')
GOLD_DEPENDENCIES = ()

statement = f"""SELECT e.*,
u.id AS sf_user_id, u.name AS sf_user_name,
        p.id AS sf_property_id, p.name AS sf_property_name,
        p.city AS sf_property_city, p.state AS sf_property_state,
        p.area_hectares AS sf_area_hectares,
        p.user_id AS sf_owner_user_id, ou.name AS sf_owner_user_name FROM {silver('property')} e

            LEFT JOIN {silver('property')} p ON p.id = e.id
            LEFT JOIN {silver('app_user')} u ON u.id = e.user_id
            LEFT JOIN {silver('app_user')} ou ON ou.id = p.user_id"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
