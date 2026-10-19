# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — _context_medicine_batch**
# MAGIC Identificar contextos de uso por lote de medicamento.
# MAGIC Esta atividade grava somente `_context_medicine_batch` e mantém a consulta original sem agregações.

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
GOLD_TABLE = '_context_medicine_batch'
SILVER_TABLES = ('application', 'herd', 'lot', 'stock_movement')
GOLD_DEPENDENCIES = ()

statement = f"""
        SELECT DISTINCT ap.medicine_batch_id, ap.user_id, h.property_id
        FROM {silver('application')} ap
        LEFT JOIN {silver('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {silver('herd')} h ON h.id = l.herd_id
        UNION
        SELECT DISTINCT sm.medicine_batch_id, sm.user_id, h.property_id
        FROM {silver('stock_movement')} sm
        LEFT JOIN {silver('application')} ap ON ap.id = sm.application_id
        LEFT JOIN {silver('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {silver('herd')} h ON h.id = l.herd_id"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
