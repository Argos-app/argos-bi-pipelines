"""Testes relacionais locais; Spark SQL é transpilado para DuckDB.

Não substituem a validação JDBC/Lakeflow no workspace do Databricks.
"""
import ast
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / '.test-deps')]
import duckdb
import sqlglot
import yaml
from argos.common import build_jdbc_options, event_expression, ingestion_mode, is_time_type, pg_ident, pg_projection
from argos.gold import gold_queries
from argos.silver import silver_query

CONTRACT = json.loads((ROOT / 'tests/schema_contract.json').read_text())


def translate(query):

    import re
    query = re.sub(r'percentile_approx\((\w+), ([0-9.]+), 10000\)',
                   r'percentile_approx(\1, \2)', query)
    return sqlglot.transpile(query, read='databricks', write='duckdb')[0]


def data_type(value):
    if value in {'UUID', 'INET', 'JSONB', 'TEXT'} or value.startswith(('VARCHAR', 'CHAR')):
        return 'VARCHAR'
    return value.replace('NUMERIC', 'DECIMAL')


class MedallionTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute("ATTACH ':memory:' AS argos")
        for schema in ('silver', 'bronze', 'self_service'):
            self.db.execute(f'CREATE SCHEMA argos.{schema}')
        for name, spec in CONTRACT.items():
            fields = ', '.join(f'"{column}" {data_type(dtype)}' for column, dtype in spec['columns'].items())
            self.db.execute(f'CREATE TABLE argos.silver.public__{name} ({fields})')
        for name, query in gold_queries('argos', 'silver', 'public', set(CONTRACT)).items():
            query = query.replace('__GOLD__', 'argos.self_service')
            self.db.execute(f'CREATE VIEW argos.self_service."{name}" AS ' + translate(query))

    def tearDown(self):
        self.db.close()

    def insert(self, table, **values):
        fields = ', '.join(f'"{column}"' for column in values)
        placeholders = ', '.join('?' for _ in values)
        self.db.execute(f'INSERT INTO argos.silver.public__{table} ({fields}) VALUES ({placeholders})', list(values.values()))

    def seed(self):
        for user in ('U1', 'U2'):
            self.insert('app_user', id=user, name=user, permission_id='PERM', active=True)
        self.insert('permission', id='PERM', code='ADMIN', name='ADMIN')
        for prop, owner in [('P1', 'U1'), ('P2', 'U2'), ('P3', 'U1')]:
            self.insert('property', id=prop, user_id=owner, name=prop, active=True)
        self.insert('user_property', user_id='U1', property_id='P1', active=True)
        self.insert('user_property', user_id='U2', property_id='P1', active=True)
        self.insert('supplier', id='S1', company_name='FORNECEDOR')
        self.insert('therapeutic_category', id='TC1', name='VACINA')
        for medicine in ('M1', 'M2'):
            self.insert('medicine', id=medicine, brand_name=medicine, supplier_id='S1',
                        therapeutic_category_id='TC1', unit='ML')
        self.insert('medicine_batch', id='MB1', medicine_id='M1', batch_number='B1')
        for index in (1, 2):
            self.insert('herd', id=f'H{index}', property_id=f'P{index}', herd_name='REBANHO',
                        head_count=10, active=True, breed='NELORE')
            self.insert('lot', id=f'L{index}', herd_id=f'H{index}', lot_code=f'L{index}', category='ENGORDA')
            self.insert('application', id=f'AP{index}', lot_id=f'L{index}', user_id=f'U{index}',
                        medicine_batch_id='MB1', application_date='2026-10-01', dose=2,
                        unit='ML', route='ORAL', head_count_applied=10, active=True)
        for index, weight in enumerate([300, 310, 320, 330, 340, 350, 360, 370, 1000], 1):
            self.insert('animal_exception', id=f'A{index}', lot_id='L1', ear_tag=f'BR{index}')
            self.insert('application_animal', id=f'AA{index}', application_id='AP1',
                        animal_exception_id=f'A{index}', weight_at_time=weight, dose_given=2)
        self.insert('stock_movement', id='SM1', medicine_batch_id='MB1', application_id='AP1',
                    user_id='U1', type='S', quantity=20, movement_date='2026-10-01')
        self.insert('stock_movement', id='SM2', medicine_batch_id='MB1', user_id='U1',
                    type='E', quantity=100, movement_date='2026-10-01')
        self.insert('occurrence', id='OC1', animal_exception_id='A1', user_id='U2',
                    occurrence_date='2026-10-02', type='DOENCA', resolved=True, resolved_at='2026-10-04')

    def dashboard(self, filename, **params):
        query = (ROOT / 'dashboards' / filename).read_text(encoding='utf-8').replace('main.self_service', 'argos.self_service')
        parsed = sqlglot.parse_one(query, read='databricks')
        parameters = {placeholder.name: None for placeholder in parsed.find_all(sqlglot.exp.Placeholder)}
        parameters.update(params)
        for key, value in list(parameters.items()):
            if key in {'usuarios', 'fazendas', 'medicamentos'}:

                parameters[key] = value
        return self.db.execute(translate(query), parameters).fetchall()

    def test_gold_all_columns_bind_to_real_model(self):
        for name in CONTRACT:
            with self.subTest(table=name):
                self.db.execute(f'SELECT * FROM argos.self_service."{name}" LIMIT 0')

    def test_no_aggregations_in_gold(self):
        for query in gold_queries('argos', 'silver', 'public', set(CONTRACT)).values():
            parsed = sqlglot.parse_one(query, read='databricks')
            self.assertFalse(list(parsed.find_all(sqlglot.exp.AggFunc)))
            self.assertFalse(list(parsed.find_all(sqlglot.exp.Group)))

    def test_facts_do_not_expand_with_user_properties_or_medicine_context(self):
        self.seed()
        rows = self.db.execute('SELECT id, sf_user_id, sf_property_id, head_count_applied FROM argos.self_service.application ORDER BY id').fetchall()
        self.assertEqual(rows, [('AP1', 'U1', 'P1', 10), ('AP2', 'U2', 'P2', 10)])
        self.assertEqual(self.db.execute('SELECT count(*) FROM argos.self_service.application_animal').fetchone()[0], 9)

    def test_medicine_context_and_unused_medicine(self):
        self.seed()
        rows = self.db.execute('SELECT id, sf_user_id, sf_property_id FROM argos.self_service.medicine ORDER BY id, sf_user_id, sf_property_id').fetchall()
        self.assertEqual(len(rows), 4)
        self.assertIn(('M2', None, None), rows)

    def test_stock_without_application_does_not_invent_a_property(self):
        self.seed()
        row = self.db.execute("SELECT sf_user_id, sf_property_id FROM argos.self_service.stock_movement WHERE id='SM2'").fetchone()
        self.assertEqual(row, ('U1', None))

    def test_owner_and_actor_are_separate(self):
        self.seed()
        row = self.db.execute('SELECT sf_user_id, sf_owner_user_id, sf_property_id FROM argos.self_service.occurrence').fetchone()
        self.assertEqual(row, ('U2', 'U1', 'P1'))

    def test_user_context_deduplicates_owner_also_linked_to_property(self):
        self.seed()
        rows = self.db.execute("SELECT sf_property_id FROM argos.self_service.app_user WHERE id='U1' ORDER BY sf_property_id").fetchall()
        self.assertEqual(rows, [('P1',), ('P3',)])

    def test_view_equivalent_includes_inactive_and_filters_before_aggregation(self):
        self.seed()
        self.insert('application', id='AP3', lot_id='L1', user_id='U2',
                    medicine_batch_id='MB1', application_date='2026-10-01',
                    head_count_applied=5, active=False)
        all_rows = self.dashboard('medicacao_13_aplicacoes_propriedade_dia.sql')
        self.assertEqual(all_rows, [('P1', 20261001, 2, 15), ('P2', 20261001, 1, 10)])
        filtered = self.dashboard('medicacao_13_aplicacoes_propriedade_dia.sql', usuarios=['U2'], fazendas=['P1'])
        self.assertEqual(filtered, [('P1', 20261001, 1, 5)])

    def test_all_dashboard_queries_bind_and_execute(self):
        self.seed()
        for path in sorted((ROOT / 'dashboards').glob('*.sql')):
            with self.subTest(dataset=path.name):
                params = {}
                if 'histograma_doses' in path.name:
                    params['largura_bin_dose'] = 1
                if 'histograma_pesos' in path.name:
                    params['largura_bin_kg'] = 50
                self.dashboard(path.name, **params)

    def test_array_contains_filters_before_aggregation(self):
        self.seed()
        rows = self.dashboard('medicacao_01_aplicacoes_temporal.sql', usuarios=['U1'], fazendas=['P1'], medicamentos=['M1'])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][6:8], (1, 10))
        self.assertEqual(self.dashboard('medicacao_01_aplicacoes_temporal.sql', usuarios=['U1'], fazendas=['P2']), [])

    def test_histogram_frequency_and_invalid_bin(self):
        self.seed()
        rows = self.dashboard('animal_05_histograma_pesos.sql', largura_bin_kg=50)
        self.assertEqual(sum(row[-1] for row in rows), 9)
        self.assertEqual(self.dashboard('animal_05_histograma_pesos.sql', largura_bin_kg=0), [])

    def test_boxplot_raw_and_tukey_outliers(self):
        self.seed()
        self.assertEqual(len(self.dashboard('animal_06_boxplot_pesos_observacoes.sql')), 9)
        summary = self.dashboard('animal_08_boxplot_pesos_estatisticas.sql')[0]
        self.assertEqual(summary[-1], 1)
        self.assertEqual(summary[-2], 370)

    def silver_setup(self):
        self.db.execute('''CREATE TABLE argos.bronze.records (
            id VARCHAR, name VARCHAR, _change_at TIMESTAMP, _ingested_at TIMESTAMP,
            _row_hash VARCHAR, _batch_id VARCHAR, _ingestion_mode VARCHAR)''')
        self.db.execute('''CREATE TABLE argos.bronze.state (
            source_schema VARCHAR, table_name VARCHAR, batch_id VARCHAR,
            mode VARCHAR, completed_at TIMESTAMP)''')

    def silver(self):
        query = silver_query('argos.bronze.records', 'argos.bronze.state', 'public', 'records',
                             ['id'], [('id', True), ('name', True)])
        return self.db.execute(translate(query)).fetchall()

    def test_silver_latest_version_upper_and_timestamp_tie(self):
        self.silver_setup()
        self.db.execute("INSERT INTO argos.bronze.state VALUES ('public','records','B2','incremental','2026-10-02')")
        self.db.execute("""INSERT INTO argos.bronze.records VALUES
            ('a','antigo','2026-10-01','2026-10-01','H1','B1','incremental'),
            ('a','novo','2026-10-01','2026-10-02','H2','B2','incremental'),
            ('a','novo','2026-10-01','2026-10-02','H2','B2','incremental')""")
        self.assertEqual(self.silver(), [('A', 'NOVO')])

    def test_empty_snapshot_and_switch_back_to_incremental_do_not_resurrect_rows(self):
        self.silver_setup()
        self.db.execute("""INSERT INTO argos.bronze.state VALUES
            ('public','records','B1','snapshot','2026-10-01'),
            ('public','records','B2','snapshot','2026-10-02')""")
        self.db.execute("INSERT INTO argos.bronze.records VALUES ('a','removido',NULL,'2026-10-01','H1','B1','snapshot')")
        self.assertEqual(self.silver(), [])
        self.db.execute("INSERT INTO argos.bronze.state VALUES ('public','records','B3','incremental','2026-10-03')")
        self.db.execute("INSERT INTO argos.bronze.records VALUES ('b','novo','2026-10-03','2026-10-03','H2','B3','incremental')")
        self.assertEqual(self.silver(), [('B', 'NOVO')])

    def test_uppercase_keys_deduplicate_and_composite_pk_keeps_properties(self):
        self.silver_setup()
        self.db.execute("INSERT INTO argos.bronze.state VALUES ('public','records','B1','incremental','2026-10-02')")
        self.db.execute("""INSERT INTO argos.bronze.records VALUES
            ('a','primeiro','2026-10-01','2026-10-01','H1','B1','incremental'),
            ('A','ultimo','2026-10-02','2026-10-02','H2','B1','incremental')""")
        self.assertEqual(self.silver(), [('A', 'ULTIMO')])
        self.db.execute('ALTER TABLE argos.bronze.records ADD COLUMN property_id VARCHAR')
        self.db.execute("UPDATE argos.bronze.records SET property_id=CASE WHEN name='primeiro' THEN 'p1' ELSE 'p2' END")
        query = silver_query('argos.bronze.records', 'argos.bronze.state', 'public', 'records',
                             ['id', 'property_id'], [('id', True), ('name', True), ('property_id', True)])
        self.assertEqual(len(self.db.execute(translate(query)).fetchall()), 2)

    def test_ingestion_policy_covers_mutable_tables_without_updated_at(self):
        for name in ('permission', 'user_property', 'device', 'alert', 'daily_access', 'load_run'):
            self.assertEqual(ingestion_mode(CONTRACT[name]['columns']), 'snapshot')
        self.assertEqual(ingestion_mode(CONTRACT['application']['columns']), 'incremental')
        self.assertEqual(ingestion_mode(CONTRACT['application']['columns'], True), 'snapshot')
        self.assertEqual(event_expression([]), 'NULL::timestamp')
        self.assertEqual(pg_ident('a"b'), '"a""b"')

    def test_direct_jdbc_credentials_preserve_case_and_special_characters(self):
        config = {'jdbc_url': 'jdbc:postgresql://host:5432/db?sslmode=require',
                  'jdbc_user': 'TesteUser', 'jdbc_password': 'Senha-Sintetica!@:/MiXeD'}
        options = build_jdbc_options(config)
        self.assertEqual(options['user'], 'TesteUser')
        self.assertEqual(options['password'], config['jdbc_password'])
        self.assertEqual(options['url'], config['jdbc_url'])

    def test_missing_credentials_fail_without_exposing_password(self):
        for password in ('', 'PREENCHA_A_SENHA_DO_BANCO'):
            with self.subTest(password_marker=bool(password)):
                with self.assertRaisesRegex(ValueError, 'Preencha jdbc_user e jdbc_password'):
                    build_jdbc_options({'jdbc_url': 'jdbc:postgresql://host/db',
                                        'jdbc_user': 'user', 'jdbc_password': password})

    def test_invalid_url_fails_without_echoing_credentials(self):
        for url in ('postgres://user:senha-sintetica@host/db',
                    'jdbc:postgresql://user:senha-sintetica@host/db'):
            with self.assertRaises(ValueError) as captured:
                build_jdbc_options({'jdbc_url': url, 'jdbc_user': 'user',
                                    'jdbc_password': 'outra-senha-sintetica'})
            self.assertNotIn('senha-sintetica', str(captured.exception))

    def test_notebook_passes_credentials_directly_without_secret_api(self):
        from types import ModuleType, SimpleNamespace
        from unittest.mock import patch
        captured, widgets = {}, {}
        module = ModuleType('argos.bronze')

        module.ingest = lambda spark, dbutils, config: captured.update(config)
        reloaded = []

        def reload_module(target):
            reloaded.append(target.__name__)
            if target is module:
                target.ingest = lambda spark, config: captured.update(config)
            return target
        fake_dbutils = SimpleNamespace(widgets=SimpleNamespace(
            text=lambda key, value: widgets.setdefault(key, value),
            get=lambda key: widgets[key]))
        source = (ROOT / 'notebooks/01_bronze.py').read_text(encoding='utf-8')
        source = source.replace('PREENCHA_A_SENHA_DO_BANCO', 'SENHA_SINTETICA')
        previous_path = list(sys.path)
        try:
            with patch.dict(sys.modules, {'argos.bronze': module}), patch('importlib.reload', side_effect=reload_module):
                exec(compile(source, '01_bronze.py', 'exec'),
                     {'spark': object(), 'dbutils': fake_dbutils})
        finally:
            sys.path[:] = previous_path
        self.assertEqual(reloaded, ['argos.common', 'argos.bronze'])
        self.assertEqual(captured['jdbc_user'], 'avnadmin')
        self.assertEqual(captured['jdbc_password'], 'SENHA_SINTETICA')
        self.assertNotIn('jdbc_user', widgets)
        self.assertNotIn('jdbc_password', widgets)


    def test_time_detection_does_not_convert_dates_or_timestamps(self):
        for name in ('TIME', 'time(6)', 'time without time zone', 'time with time zone', 'timetz'):
            self.assertTrue(is_time_type(name))
        for name in ('timestamp', 'timestamp_ntz', 'timestamp with time zone', 'date', 'string', 'numeric'):
            self.assertFalse(is_time_type(name))

    def test_source_time_projection_preserves_microseconds_nulls_and_timestamp_type(self):
        columns = ['id', 'application_time', 'created_at']
        types = {'id': 'integer', 'application_time': 'time without time zone', 'created_at': 'timestamp without time zone'}
        projection = pg_projection(columns, types)
        source = """(VALUES
            (1, TIME '09:15:30.123456', TIMESTAMP '2026-10-14 09:15:30'),
            (2, CAST(NULL AS TIME), TIMESTAMP '2026-10-14 10:00:00')
        ) AS src(id, application_time, created_at)"""
        rows = self.db.execute('SELECT ' + projection + ' FROM ' + source + ' ORDER BY id').fetchall()
        self.assertEqual(rows[0][1], '09:15:30.123456')
        self.assertIsNone(rows[1][1])
        self.assertEqual(rows[0][2].year, 2026)
        self.assertEqual(pg_projection(['a"b'], {'a"b': 'time'}), 'CAST("a""b" AS TEXT) AS "a""b"')


    def test_python_syntax_and_bundle_orchestration(self):
        for path in ROOT.rglob('*.py'):
            if '.test-deps' not in path.parts:
                ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        bundle = yaml.safe_load((ROOT / 'databricks.yml').read_text())
        job = bundle['resources']['jobs']['argos_medallion']
        self.assertEqual(job['max_concurrent_runs'], 1)
        self.assertEqual([task['task_key'] for task in job['tasks'][:2]], ['bronze', 'silver'])
        self.assertEqual(job['tasks'][-1]['task_key'], 'validar')
        self.assertNotIn('pipelines', bundle['resources'])
        self.assertNotIn('gold', [task['task_key'] for task in job['tasks']])
        self.assertTrue(all(task['task_key'].startswith('gold_') for task in job['tasks'][2:-1]))
        for task in job['tasks']:
            self.assertIn('notebook_task', task)
            self.assertNotIn('pipeline_task', task)
            self.assertTrue((ROOT / task['notebook_task']['notebook_path']).is_file())


if __name__ == '__main__':
    unittest.main(verbosity=2)
