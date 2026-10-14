# Databricks notebook source
# MAGIC %md
# MAGIC Validação no Databricks após atualização da pipeline.

# COMMAND ----------
import sys
from pathlib import Path
from pyspark.sql import functions as F

sys.path.insert(0, str(Path.cwd().parent))
from argos.common import source_name, table_name

for key, default in {'catalog': 'main', 'bronze_schema': 'bronze',
                     'silver_schema': 'silver', 'gold_schema': 'self_service'}.items():
    dbutils.widgets.text(key, default)
config = {key: dbutils.widgets.get(key) for key in ('catalog', 'bronze_schema', 'silver_schema', 'gold_schema')}
catalog = config['catalog']
manifest = spark.table(table_name(catalog, config['bronze_schema'], '_table_manifest')).collect()
facts = {'property', 'user_property', 'herd', 'lot', 'animal_exception', 'application',
         'application_animal', 'stock_movement', 'occurrence', 'animal_lot_history',
         'alert', 'notification_delivery', 'attachment'}
errors = []
for item in manifest:
    name = item.table_name
    silver = spark.table(table_name(catalog, config['silver_schema'], source_name(item.source_schema, name)))
    if item.primary_keys:
        duplicate = silver.groupBy(*item.primary_keys).count().where('count > 1').limit(1).count()
        null_condition = None
        for key in item.primary_keys:
            condition = F.col(key).isNull()
            null_condition = condition if null_condition is None else null_condition | condition
        null_pk = silver.where(null_condition).limit(1).count()
        if duplicate or null_pk:
            errors.append(f'{name}: PK duplicada ou nula na Silver')
    # Gold usa o schema de origem configurado na pipeline; apenas um schema por Job.
    gold = spark.table(table_name(catalog, config['gold_schema'], name))
    silver_count, gold_count = silver.count(), gold.count()
    if name in facts and gold_count != silver_count:
        errors.append(f'{name}: Gold {gold_count} != Silver {silver_count}; possível multiplicação no join')
    if gold_count < silver_count:
        errors.append(f'{name}: registros perdidos no join')
    print(f'{name}: Silver={silver_count}, Self Service={gold_count}')
if errors:
    raise AssertionError('\n'.join(errors))
print('Validação concluída: PKs válidas e granularidade preservada.')
