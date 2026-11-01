"""Testes da criação inicial e gravação batch, com um adaptador Spark em memória.

A semântica das queries é exercitada separadamente em test_medallion.py.
"""
import re
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from argos.common import table_name
from argos.delta_layers import assert_delta_targets, run_silver, run_gold_statement, write_snapshot


class Row(dict):
    def __getattr__(self, key):
        return self[key]

    def asDict(self):
        return dict(self)


def key(name):
    return name.replace('`', '')


class Writer:
    def __init__(self, frame):
        self.frame, self.format_name, self.mode_name, self.options = frame, None, None, {}

    def format(self, value):
        self.format_name = value
        return self

    def mode(self, value):
        self.mode_name = value
        return self

    def option(self, name, value):
        self.options[name] = value
        return self

    def saveAsTable(self, target):
        target = key(target)
        spark = self.frame.spark
        if target in spark.fail_writes:
            raise RuntimeError('Falha de escrita simulada')
        if self.mode_name == 'errorifexists' and target in spark.objects:
            raise ValueError('Destino já existe')
        spark.objects[target] = {'table_type': 'MANAGED', 'data_source_format': 'DELTA', 'table_owner': 'TEST'}
        spark.data[target] = list(self.frame.rows)
        spark.events.append(('write', target, self.mode_name, self.format_name, dict(self.options)))


class Frame:
    def __init__(self, spark, rows=()):
        self.spark, self.rows = spark, list(rows)
        self.schema = SimpleNamespace(fields=[SimpleNamespace(name='id', dataType=SimpleNamespace(typeName=lambda: 'string'))])

    @property
    def write(self):
        return Writer(self)

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0] if self.rows else None

    def count(self):
        return len(self.rows)


class MiniSpark:
    def __init__(self):
        self.objects, self.data, self.events, self.fail_writes = {}, {}, [], set()
        self.conf = SimpleNamespace(set=lambda *args: None)
        self.next_snapshot = [Row(id='CURRENT')]

    def register(self, schema, name, rows=(), kind='MANAGED', fmt='DELTA'):
        target = f'argos.{schema}.{name}'
        self.objects[target] = {'table_type': kind, 'data_source_format': fmt, 'table_owner': 'TEST'}
        self.data[target] = list(rows)
        return target

    def table(self, name):
        return Frame(self, self.data[key(name)])

    def createDataFrame(self, rows, schema):
        return Frame(self, rows)

    def sql(self, statement):
        self.events.append(('sql', statement))
        normalized = key(statement)
        if 'information_schema.tables' in normalized:
            schema = re.search(r"table_schema = '([^']+)'", statement)[1]
            name = re.search(r"table_name = '([^']+)'", statement)[1]
            info = self.objects.get(f'argos.{schema}.{name}')
            return Frame(self, [Row(info)] if info else [])
        if normalized.startswith('CREATE SCHEMA'):
            return Frame(self)
        if normalized.startswith('SELECT DISTINCT source_schema'):
            return Frame(self, self.data['argos.bronze._ingestion_state'])

        match = re.fullmatch(r'SELECT .* FROM (argos\.[\w]+\.[\w]+)', normalized, re.S)
        if match and match[1] in self.data:
            return self.table(match[1])
        return Frame(self, self.next_snapshot)


CONFIG = {'catalog': 'argos', 'source_schema': 'public', 'bronze_schema': 'bronze',
          'silver_schema': 'silver', 'gold_schema': 'self_service'}
ARGOS_TABLES = {'permission', 'app_user', 'property', 'user_property', 'herd', 'lot',
                'animal_exception', 'application', 'application_animal', 'medicine', 'medicine_batch',
                'supplier', 'therapeutic_category', 'alert', 'stock_movement'}


def setup(spark, tables):
    manifest = [Row(source_schema='public', table_name=name, primary_keys=['id']) for name in sorted(tables)]
    spark.register('bronze', '_table_manifest', manifest)
    spark.register('bronze', '_ingestion_state', manifest)
    for name in tables:
        spark.register('bronze', 'public__' + name, [Row(id='OLD')])
        spark.register('silver', 'public__' + name, [Row(id='OLD')])


class DeltaTableTests(unittest.TestCase):
    def test_silver_creates_schema_and_table_when_destination_does_not_exist(self):
        spark = MiniSpark()
        manifest = [Row(source_schema='public', table_name='records', primary_keys=['id'])]
        spark.register('bronze', '_table_manifest', manifest)
        spark.register('bronze', '_ingestion_state', manifest)
        spark.register('bronze', 'public__records', [Row(id='RAW')])
        self.assertNotIn('argos.silver.public__records', spark.objects)
        run_silver(spark, CONFIG)
        self.assertEqual(spark.data['argos.silver.public__records'], [Row(id='CURRENT')])
        creation = next(i for i, event in enumerate(spark.events)
                        if event[0] == 'sql' and event[1] == 'CREATE SCHEMA IF NOT EXISTS `argos`.`silver`')
        first_write = next(i for i, event in enumerate(spark.events) if event[0] == 'write')
        self.assertLess(creation, first_write)

    def test_gold_creates_schema_and_single_table_when_destination_does_not_exist(self):
        spark = MiniSpark()
        spark.register('silver', 'public__records', [Row(id='CURRENT')])
        run_gold_statement(spark, CONFIG, 'records', 'SELECT id FROM argos.silver.public__records', ('records',))
        self.assertEqual(spark.data['argos.self_service.records'], [Row(id='CURRENT')])
        creation = next(i for i, event in enumerate(spark.events)
                        if event[0] == 'sql' and event[1] == 'CREATE SCHEMA IF NOT EXISTS `argos`.`self_service`')
        first_write = next(i for i, event in enumerate(spark.events) if event[0] == 'write')
        self.assertLess(creation, first_write)

    def test_write_snapshot_is_delta_overwrite_and_empty_snapshot_clears_old_rows(self):
        spark = MiniSpark()
        spark.register('silver', 'records', [Row(id='OLD')])
        write_snapshot(Frame(spark), table_name('argos', 'silver', 'records'))
        self.assertEqual(spark.data['argos.silver.records'], [])
        self.assertEqual(spark.events[-1][2:], ('overwrite', 'delta', {'overwriteSchema': 'true'}))

    def test_silver_runs_as_batch_table_and_does_not_append_duplicates(self):
        spark = MiniSpark()
        setup(spark, {'records'})
        run_silver(spark, CONFIG)
        run_silver(spark, CONFIG)
        self.assertEqual(spark.data['argos.silver.public__records'], [Row(id='CURRENT')])
        self.assertEqual(spark.objects['argos.silver.public__records']['table_type'], 'MANAGED')
        spark.next_snapshot = []
        run_silver(spark, CONFIG)
        self.assertEqual(spark.data['argos.silver.public__records'], [])

    def test_existing_mv_is_blocked_before_any_silver_write(self):
        spark = MiniSpark()
        setup(spark, {'records'})
        spark.objects['argos.silver.public__records']['table_type'] = 'MATERIALIZED_VIEW'
        with self.assertRaisesRegex(ValueError, 'Delta comuns'):
            run_silver(spark, CONFIG)
        self.assertFalse(any(event[0] == 'write' for event in spark.events))

    def test_non_delta_table_and_missing_required_table_are_rejected(self):
        spark = MiniSpark()
        spark.register('silver', 'records', fmt='PARQUET')
        with self.assertRaises(ValueError):
            assert_delta_targets(spark, 'argos', [('silver', 'records')])
        with self.assertRaises(ValueError):
            assert_delta_targets(spark, 'argos', [('silver', 'missing')], require_exists=True)






if __name__ == '__main__':
    unittest.main(verbosity=2)
