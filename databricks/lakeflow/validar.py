# Databricks notebook source
from pyspark.sql import functions as F

for key, value in {
    'catalog': 'main', 'source_schema': 'public', 'bronze_schema': 'bronze',
    'silver_schema': 'silver', 'gold_schema': 'self_service',
}.items():
    dbutils.widgets.text(key, value)
config = {key: dbutils.widgets.get(key) for key in (
    'catalog', 'source_schema', 'bronze_schema', 'silver_schema', 'gold_schema'
)}

def table(schema, name):
    def quote(value):
        return '`' + value.replace('`', '``') + '`'
    return '.'.join(quote(value) for value in (config['catalog'], schema, name))

manifest = spark.table(table(config['bronze_schema'], '_table_manifest')).collect()
if not manifest:
    raise ValueError('Manifesto Bronze vazio.')
expected_tables = {'interaction_audit', 'medicine_batch', 'access_audit', 'occurrence', 'herd', 'stock_movement', 'load_staging', 'animal_exception', 'application_animal', 'load_run', 'daily_access', 'lot', 'load_error', 'load_mapping', 'medicine', 'user_property', 'dim_date', 'data_catalog', 'animal_lot_history', 'supplier', 'attachment', 'device', 'property', 'notification_delivery', 'alert', 'change_audit', 'app_user', 'permission', 'therapeutic_category', 'application'}
source_tables = {item.table_name for item in manifest if item.source_schema == config['source_schema']}
missing = expected_tables - source_tables
if missing:
    raise ValueError('Tabelas obrigatórias ausentes no manifesto Bronze: ' + ', '.join(sorted(missing)))
facts = {'property', 'user_property', 'herd', 'lot', 'animal_exception', 'application',
         'application_animal', 'stock_movement', 'occurrence', 'animal_lot_history',
         'alert', 'notification_delivery', 'attachment'}
for item in manifest:
    if item.source_schema != config['source_schema'] or item.table_name not in expected_tables:
        continue
    silver = spark.table(table(config['silver_schema'], item.source_schema + '__' + item.table_name))
    keys = item.primary_keys
    if keys:
        nulls = None
        for key in keys:
            condition = F.col(key).isNull()
            nulls = condition if nulls is None else nulls | condition
        if silver.filter(nulls).limit(1).count() or silver.groupBy(*keys).count().filter('count > 1').limit(1).count():
            raise ValueError('Chave nula ou duplicada na Silver: ' + item.table_name)
    if item.table_name in facts:
        gold = spark.table(table(config['gold_schema'], item.table_name))
        if gold.count() != silver.count():
            raise ValueError('Cardinalidade Gold divergente: ' + item.table_name)
print('Validação de chaves Silver e cardinalidade Gold concluída.')
