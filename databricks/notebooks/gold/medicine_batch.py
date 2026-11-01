# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — medicine_batch**
# MAGIC Disponibilizar lotes de medicamentos, validade e contextos de uso.
# MAGIC Esta atividade grava somente `medicine_batch` e mantém a consulta original sem agregações.

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
GOLD_TABLE = 'medicine_batch'
SILVER_TABLES = ('app_user', 'medicine', 'medicine_batch', 'property', 'supplier', 'therapeutic_category')
GOLD_DEPENDENCIES = ('_context_medicine_batch',)

statement = f"""SELECT e.*,
u.id AS sf_user_id, u.name AS sf_user_name,
        p.id AS sf_property_id, p.name AS sf_property_name,
        p.city AS sf_property_city, p.state AS sf_property_state,
        p.area_hectares AS sf_area_hectares,
        p.user_id AS sf_owner_user_id, ou.name AS sf_owner_user_name,
mb.id AS sf_medicine_batch_id, mb.batch_number AS sf_batch_number,
        mb.expiry_date AS sf_expiry_date, m.id AS sf_medicine_id,
        m.brand_name AS sf_medicine_name, m.active_ingredient AS sf_active_ingredient,
        m.unit AS sf_medicine_unit, m.meat_withdrawal_days AS sf_withdrawal_days,
        s.id AS sf_supplier_id, s.company_name AS sf_supplier_name,
        tc.id AS sf_therapeutic_category_id, tc.name AS sf_therapeutic_category FROM {silver('medicine_batch')} e
LEFT JOIN {gold('_context_medicine_batch')} c ON c.medicine_batch_id = e.id
LEFT JOIN {silver('medicine_batch')} mb ON mb.id = e.id
            LEFT JOIN {silver('medicine')} m ON m.id = mb.medicine_id
            LEFT JOIN {silver('supplier')} s ON s.id = m.supplier_id
            LEFT JOIN {silver('therapeutic_category')} tc ON tc.id = m.therapeutic_category_id
            LEFT JOIN {silver('property')} p ON p.id = c.property_id
            LEFT JOIN {silver('app_user')} u ON u.id = c.user_id
            LEFT JOIN {silver('app_user')} ou ON ou.id = p.user_id"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
