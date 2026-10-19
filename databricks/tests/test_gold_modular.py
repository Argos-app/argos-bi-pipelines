"""Paridade dos notebooks Gold e dependências no único Job existente."""
import ast
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / '.test-deps')]
import sqlglot
import yaml
import argos.delta_layers as layers
from argos.gold import gold_queries
import test_medallion as sql_tests
from test_medallion import CONTRACT, translate
from test_delta_tables import CONFIG, MiniSpark, Row


def capture_notebook(path):
    values = dict(CONFIG)
    widgets = SimpleNamespace(text=lambda key, value: values.setdefault(key, value),
                              get=lambda key: values[key])
    captured = {}

    def capture(spark, config, name, statement, silver_tables, gold_dependencies=()):
        captured.update(name=name, statement=statement, silver_tables=tuple(silver_tables),
                        gold_dependencies=tuple(gold_dependencies))

    previous_path = list(sys.path)
    try:
        with patch('importlib.reload', side_effect=lambda module: module), patch.object(layers, 'run_gold_statement', side_effect=capture):
            exec(compile(path.read_text(encoding='utf-8'), str(path), 'exec'),
                 {'spark': object(), 'dbutils': SimpleNamespace(widgets=widgets)})
    finally:
        sys.path[:] = previous_path
    return captured


def references(statement):
    parsed = sqlglot.parse_one(statement, read='databricks')
    silver, gold = set(), set()
    for table in parsed.find_all(sqlglot.exp.Table):
        if table.db == 'silver':
            silver.add(table.name.removeprefix('public__'))
        elif table.db == 'self_service':
            gold.add(table.name)
    return silver, gold


class ModularGoldTests(unittest.TestCase):
    def test_each_existing_output_has_own_notebook_with_identical_sql_and_exact_dependencies(self):
        expected = gold_queries('argos', 'silver', 'public', set(CONTRACT))
        files = list((ROOT / 'notebooks/gold').glob('*.py'))
        self.assertTrue(set(expected) <= {path.stem for path in files})
        for name, statement in expected.items():
            with self.subTest(table=name):
                captured = capture_notebook(ROOT / 'notebooks/gold' / (name + '.py'))
                expected_sql = statement.replace('__GOLD__', '`argos`.`self_service`')
                self.assertEqual(captured['name'], name)
                # Identificadores de helpers agora recebem aspas de forma uniforme.
                # Compara a árvore SQL renderizada com identificação completa.
                self.assertEqual(
                    sqlglot.parse_one(captured['statement'], read='databricks').sql(dialect='databricks', identify=True),
                    sqlglot.parse_one(expected_sql, read='databricks').sql(dialect='databricks', identify=True))
                sources, helpers = references(expected_sql)
                self.assertEqual(set(captured['silver_tables']), sources)
                self.assertEqual(set(captured['gold_dependencies']), helpers)

    def test_single_existing_job_contains_all_gold_tasks_with_success_dependencies_and_no_cycles(self):
        bundle = yaml.safe_load((ROOT / 'databricks.yml').read_text(encoding='utf-8'))
        self.assertNotIn('pipelines', bundle['resources'])
        self.assertEqual(set(bundle['resources']['jobs']), {'argos_medallion'})
        job = bundle['resources']['jobs']['argos_medallion']
        tasks = {task['task_key']: task for task in job['tasks']}
        expected = gold_queries('argos', 'silver', 'public', set(CONTRACT))
        notebooks = {path.stem: path for path in (ROOT / 'notebooks/gold').glob('*.py')}
        gold_keys = {'gold_' + name for name in notebooks}
        self.assertTrue({'gold_' + name for name in expected} <= gold_keys)
        self.assertEqual(set(tasks), {'bronze', 'silver', 'validar'} | gold_keys)
        for name in notebooks:
            task = tasks['gold_' + name]
            captured = capture_notebook(ROOT / 'notebooks/gold' / (name + '.py'))
            self.assertEqual(task['notebook_task']['notebook_path'], './notebooks/gold/' + name + '.py')
            actual_deps = {dependency['task_key'] for dependency in task['depends_on']}
            self.assertTrue(({'silver'} | {'gold_' + helper for helper in captured['gold_dependencies']}) <= actual_deps)
            self.assertEqual(task['run_if'], 'ALL_SUCCESS')
        self.assertEqual({dep['task_key'] for dep in tasks['validar']['depends_on']}, gold_keys)
        completed = set()
        while len(completed) < len(tasks):
            ready = {name for name, task in tasks.items()
                     if name not in completed and {dep['task_key'] for dep in task.get('depends_on', [])} <= completed}
            self.assertTrue(ready, 'Ciclo ou dependência inexistente no Job')
            completed.update(ready)

    def batch_configuration(self):
        bundle = yaml.safe_load((ROOT / 'databricks.yml').read_text(encoding='utf-8'))
        job = bundle['resources']['jobs']['argos_medallion']
        tasks = {task['task_key']: task for task in job['tasks']}
        gold = [name for name in tasks if name.startswith('gold_')]
        batches = [gold[i:i + 5] for i in range(0, len(gold), 5)]
        return job, tasks, batches

    def test_gold_batches_have_full_success_barriers_and_preserve_business_dependencies(self):
        job, tasks, batches = self.batch_configuration()
        self.assertEqual(job['max_concurrent_runs'], 1)
        self.assertEqual([len(batch) for batch in batches], [5] * 7)
        for index, batch in enumerate(batches):
            previous = set(batches[index - 1]) if index else set()
            for name in batch:
                notebook = ROOT / tasks[name]['notebook_task']['notebook_path']
                captured = capture_notebook(notebook)
                business = {'silver'} | {'gold_' + helper for helper in captured['gold_dependencies']}
                self.assertEqual({d['task_key'] for d in tasks[name]['depends_on']}, business | previous)
                self.assertEqual(tasks[name]['run_if'], 'ALL_SUCCESS')

    def test_async_task_durations_cannot_overlap_gold_batches_or_exceed_five(self):
        import random
        _, tasks, batches = self.batch_configuration()
        membership = {name: index for index, batch in enumerate(batches) for name in batch}
        for seed in range(30):
            rng = random.Random(seed)
            completed, active = set(), {}
            clock = 0
            while len(completed) < len(tasks):
                ready = [name for name, task in tasks.items() if name not in completed and name not in active
                         and {d['task_key'] for d in task.get('depends_on', [])} <= completed]
                for name in ready:
                    active[name] = clock + rng.randint(1, 100)
                self.assertTrue(active, 'DAG sem progresso')
                running_gold = [name for name in active if name in membership]
                self.assertLessEqual(len(active), 5)
                self.assertLessEqual(len({membership[name] for name in running_gold}), 1)
                clock = min(active.values())
                finished = {name for name, end in active.items() if end == clock}
                completed.update(finished)
                active = {name: end for name, end in active.items() if name not in finished}

    def test_failure_in_any_batch_blocks_later_batches_and_validation(self):
        _, tasks, batches = self.batch_configuration()
        for index, batch in enumerate(batches):
            for failed in batch:
                successful = {'bronze', 'silver'}
                while True:
                    ready = {name for name, task in tasks.items() if name not in successful and name != failed
                             and {d['task_key'] for d in task.get('depends_on', [])} <= successful}
                    if not ready:
                        break
                    successful.update(ready)
                later = {name for following in batches[index + 1:] for name in following}
                self.assertFalse(later & successful)
                self.assertNotIn('validar', successful)

    def test_notebook_queries_return_same_rows_and_schema_as_previous_gold(self):
        # Usa o mesmo banco sintético que verifica as 30 tabelas e cardinalidades.
        fixture = sql_tests.MedallionTests()
        fixture.setUp()
        try:
            fixture.seed()
            expected = gold_queries('argos', 'silver', 'public', set(CONTRACT))
            for name, statement in expected.items():
                with self.subTest(table=name):
                    captured = capture_notebook(ROOT / 'notebooks/gold' / (name + '.py'))
                    old_sql = translate(statement.replace('__GOLD__', 'argos.self_service'))
                    old_cursor = fixture.db.execute(old_sql)
                    old_columns = [column[0] for column in old_cursor.description]
                    old_rows = old_cursor.fetchall()
                    new_cursor = fixture.db.execute(translate(captured['statement']))
                    self.assertEqual([column[0] for column in new_cursor.description], old_columns)
                    self.assertCountEqual(new_cursor.fetchall(), old_rows)
        finally:
            fixture.tearDown()

    def test_single_runner_writes_only_selected_table_and_requires_only_its_inputs(self):
        spark = MiniSpark()
        spark.register('silver', 'public__lot', [Row(id='L1')])
        layers.run_gold_statement(spark, CONFIG, 'lotes_para_analise',
                                  'SELECT id FROM argos.silver.public__lot', ('lot',))
        writes = [event for event in spark.events if event[0] == 'write']
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][1], 'argos.self_service.lotes_para_analise')
        self.assertEqual(spark.data['argos.self_service.lotes_para_analise'], [Row(id='L1')])

    def test_missing_context_blocks_selected_output_without_writing_other_tables(self):
        spark = MiniSpark()
        spark.register('silver', 'public__medicine')
        with self.assertRaises(ValueError):
            layers.run_gold_statement(spark, CONFIG, 'medicine', 'SELECT 1',
                                      ('medicine',), ('_context_medicine',))
        self.assertFalse(any(event[0] == 'write' for event in spark.events))

    def test_new_gold_only_needs_its_notebook_and_a_task_not_shared_query_registration(self):
        spark = MiniSpark()
        spark.register('silver', 'public__application', [Row(id='AP1')])
        with patch.object(layers, 'gold_queries', side_effect=AssertionError('Novo dataset não deve depender do registro central')):
            layers.run_gold_statement(spark, CONFIG, 'novo_objetivo_negocio',
                                      'SELECT id FROM argos.silver.public__application', ('application',))
        self.assertEqual(spark.data['argos.self_service.novo_objetivo_negocio'], [Row(id='AP1')])


if __name__ == '__main__':
    unittest.main(verbosity=2)
