# Databricks notebook source
# MAGIC %md
# MAGIC **Self Service — notification_delivery**
# MAGIC Disponibilizar entregas de notificações e o contexto do alerta.
# MAGIC Esta atividade grava somente `notification_delivery` e mantém a consulta original sem agregações.

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
GOLD_TABLE = 'notification_delivery'
SILVER_TABLES = ('alert', 'animal_exception', 'app_user', 'herd', 'lot', 'notification_delivery', 'property')
GOLD_DEPENDENCIES = ()

statement = f"""SELECT e.*,
u.id AS sf_user_id, u.name AS sf_user_name,
        p.id AS sf_property_id, p.name AS sf_property_name,
        p.city AS sf_property_city, p.state AS sf_property_state,
        p.area_hectares AS sf_area_hectares,
        p.user_id AS sf_owner_user_id, ou.name AS sf_owner_user_name,
l.id AS sf_lot_id, l.lot_code AS sf_lot_code, l.category AS sf_lot_category,
        h.id AS sf_herd_id, h.herd_name AS sf_herd_name, h.breed AS sf_breed FROM {silver('notification_delivery')} e
LEFT JOIN {silver('alert')} a ON a.id = e.alert_id
LEFT JOIN {silver('animal_exception')} ae ON ae.id = a.animal_exception_id
LEFT JOIN {silver('lot')} l ON l.id = coalesce(a.lot_id, ae.lot_id)
                   LEFT JOIN {silver('herd')} h ON h.id = l.herd_id
            LEFT JOIN {silver('property')} p ON p.id = h.property_id
            LEFT JOIN {silver('app_user')} u ON u.id = a.user_id
            LEFT JOIN {silver('app_user')} ou ON ou.id = p.user_id"""

layers.run_gold_statement(spark, config, GOLD_TABLE, statement, SILVER_TABLES, GOLD_DEPENDENCIES)
