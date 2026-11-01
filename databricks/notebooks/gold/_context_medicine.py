# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — _context_medicine**
# MAGIC Identificar contextos de uso de medicamentos em aplicações e movimentações.
# MAGIC Esta atividade grava somente `_context_medicine` e mantém a consulta original sem agregações.

# COMMAND ----------
import sys
import importlib
from pathlib import Path


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
GOLD_TABLE = '_context_medicine'
SILVER_TABLES = ('application', 'herd', 'lot', 'medicine_batch', 'stock_movement')
GOLD_DEPENDENCIES = ()

statement = f"""
        SELECT DISTINCT mb.medicine_id, ap.user_id, h.property_id
        FROM {silver('application')} ap
        LEFT JOIN {silver('medicine_batch')} mb ON mb.id = ap.medicine_batch_id
        LEFT JOIN {silver('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {silver('herd')} h ON h.id = l.herd_id
        UNION
        SELECT DISTINCT mb.medicine_id, sm.user_id, h.property_id
        FROM {silver('stock_movement')} sm
        LEFT JOIN {silver('medicine_batch')} mb ON mb.id = sm.medicine_batch_id
        LEFT JOIN {silver('application')} ap ON ap.id = sm.application_id
        LEFT JOIN {silver('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {silver('herd')} h ON h.id = l.herd_id"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
