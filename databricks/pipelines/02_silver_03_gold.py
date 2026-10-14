"""Fonte Lakeflow: somente definições; JDBC e writes ficam no Job Bronze."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark import pipelines as dp
from pyspark.sql.types import StringType

from argos.common import source_name, table_name
from argos.gold import gold_queries
from argos.silver import silver_query

catalog = spark.conf.get('argos.catalog', 'main')
bronze_schema = spark.conf.get('argos.bronze_schema', 'bronze')
silver_schema = spark.conf.get('argos.silver_schema', 'silver')
gold_schema = spark.conf.get('argos.gold_schema', 'self_service')
source_schema = spark.conf.get('argos.source_schema', 'public')
state = table_name(catalog, bronze_schema, '_ingestion_state')

# Leitura de metadados no planejamento, fora das funções de transformação.
# Execute Bronze antes da primeira atualização desta pipeline.
manifest = spark.table(table_name(catalog, bronze_schema, '_table_manifest')).collect()
for item in manifest:
    bronze = table_name(catalog, bronze_schema, source_name(item.source_schema, item.table_name))
    target = table_name(catalog, silver_schema, source_name(item.source_schema, item.table_name))
    fields = [(f.name, isinstance(f.dataType, StringType)) for f in spark.table(bronze).schema.fields]
    query = silver_query(bronze, state, item.source_schema, item.table_name, item.primary_keys, fields)

    @dp.materialized_view(name=target, comment='Argos: UPPER e última versão por PK')
    def silver_dataset(statement=query):
        return spark.sql(statement)

available = {item.table_name for item in manifest if item.source_schema == source_schema}
gold_prefix = table_name(catalog, gold_schema, 'x').rsplit('.', 1)[0]
for name, statement in gold_queries(catalog, silver_schema, source_schema, available).items():
    statement = statement.replace('__GOLD__', gold_prefix)
    @dp.materialized_view(name=table_name(catalog, gold_schema, name),
                          comment='Argos Self Service: joins sem agregação')
    def gold_dataset(query=statement):
        return spark.sql(query)
