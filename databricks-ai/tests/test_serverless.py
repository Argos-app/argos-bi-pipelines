"""Contratos serverless, staging com falhas e ingestão JSONL sem rede/Spark."""
import ast
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from argos import bronze
from argos.common import DEFAULTS, sources, config_signature, validate_jsonl_path
from sample_records import fixture_documents
from argos.parsing import parse_record
from argos.staging import DeltaStage

class Catalog:
    def __init__(self):
        self.names = set()
    def tableExists(self, target):
        return target in self.names

class Writer:
    def __init__(self, spark, value):
        self.spark, self.value = spark, value
    def format(self, value):
        assert value == 'delta'
        return self
    def mode(self, value):
        assert value == 'errorifexists'
        return self
    def saveAsTable(self, target):
        assert target not in self.spark.catalog.names
        self.spark.catalog.names.add(target)
        self.spark.frames[target] = copy.deepcopy(self.value)

class Spark:
    def __init__(self):
        self.catalog, self.frames, self.statements = Catalog(), {}, []
    def frame(self, value):
        return SimpleNamespace(write=Writer(self, value))
    def table(self, target):
        return self.frames[target]
    def createDataFrame(self, rows, schema):
        return rows
    def sql(self, text):
        self.statements.append(text)
        if text.startswith('DROP TABLE IF EXISTS '):
            target = text.removeprefix('DROP TABLE IF EXISTS ')
            self.catalog.names.discard(target)
            self.frames.pop(target, None)

class ServerlessTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(DEFAULTS)
        for key in ('page_size','max_documents_per_source','max_page_bytes'):
            self.config[key] = int(self.config[key])
        self.config['page_size'] = 2
        self.documents, self.ids = fixture_documents()

    def test_staging_reads_new_delta_tables_and_cleans_on_exception(self):
        spark = Spark()
        with self.assertRaisesRegex(RuntimeError, 'failure'):
            with DeltaStage(spark, self.config) as stage:
                self.assertEqual(stage.materialize(spark.frame([1])), [1])
                self.assertEqual(stage.materialize(spark.frame([2])), [2])
                self.assertEqual(len(spark.catalog.names), 2)
                self.assertTrue(all('_work_' in name for name in spark.catalog.names))
                raise RuntimeError('failure')
        self.assertEqual(spark.catalog.names, set())
        self.assertEqual(len(spark.statements), 2)

    def test_staging_does_not_overwrite_existing_tables(self):
        spark = Spark()
        stage = DeltaStage(spark, self.config)
        from argos.common import table
        target = table(self.config, 'silver', stage.prefix + '_0')
        spark.catalog.names.add(target)
        with self.assertRaises(RuntimeError):
            stage.materialize(spark.frame([1]))
        self.assertEqual(stage.targets, [])

    def test_cleanup_error_keeps_original_failure(self):
        spark = Spark()
        with self.assertWarns(RuntimeWarning), self.assertRaisesRegex(ValueError, 'original'):
            with DeltaStage(spark, self.config) as stage:
                stage.materialize(spark.frame([1]))
                with patch.object(spark, 'sql', side_effect=RuntimeError('cleanup')):
                    stage.__exit__(ValueError, ValueError('original'), None)
                raise ValueError('original')

    def write_files(self, folder):
        workflows = []
        for alias, documents in self.documents.items():
            if alias in bronze.WORKFLOW_TYPES:
                workflows.extend(documents)
            else:
                lines = [bronze.canonical_json(document) for document in documents]
                (folder/(alias + '.jsonl')).write_text('\n'.join(lines), encoding='utf-8')
        (folder/'email_workflows.jsonl').write_text('\n'.join(map(bronze.canonical_json, workflows)), encoding='utf-8')

    def test_jsonl_roundtrip_bson_and_all_source_contracts(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            self.write_files(folder)
            for alias, expected in self.documents.items():
                path = folder/(('email_workflows' if alias in bronze.WORKFLOW_TYPES else alias)+'.jsonl')
                rows = list(bronze.iter_jsonl_documents(path, 1000000))
                if alias in bronze.WORKFLOW_TYPES:
                    rows = [(pos, doc) for pos, doc in rows if doc['record_type'] == bronze.WORKFLOW_TYPES[alias]]
                self.assertEqual([doc for position, doc in rows], expected)
                for position, document in rows:
                    self.assertEqual(parse_record(alias, bronze.canonical_json(document))['errors'], [])

    def test_invalid_jsonl_and_oversize_document_fail_without_payload_in_error(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'invalid.jsonl'
            for content, limit in (('SECRET_INVALID_JSON', 1000), ('[1,2]', 1000), ('x'*100, 20)):
                path.write_text(content, encoding='utf-8')
                with self.assertRaises(ValueError) as result:
                    list(bronze.iter_jsonl_documents(path, limit))
                self.assertNotIn('SECRET', str(result.exception))

    def test_jsonl_ingestion_commits_all_sources_and_preserves_file_lineage(self):
        with tempfile.TemporaryDirectory() as temp:
            self.write_files(Path(temp))
            config = dict(self.config, jsonl_path=temp)
            spark, pages, appends = Spark(), [], []
            with patch.object(bronze, 'ensure_schemas'), patch.object(bronze, 'assert_delta'), \
                 patch.object(bronze, 'ensure_bronze_table'), \
                 patch.object(bronze, 'merge_page', side_effect=lambda s,t,rows: pages.append((t, copy.deepcopy(rows)))), \
                 patch.object(bronze, 'append_frame', side_effect=lambda s,rows,t: appends.append((t, rows))), \
                 patch('pymongo.MongoClient') as mongo:
                bronze.ingest(spark, config)
                mongo.assert_not_called()
            states = [rows[0] for target, rows in appends if '_ingestion_state' in target]
            self.assertEqual(len(states), 15)
            self.assertTrue(all(state['is_snapshot'] for state in states))
            self.assertEqual(states[5]['documents'], 0)  # A2A vazio é um snapshot válido.
            self.assertEqual(len([t for t,r in appends if t.endswith('`_runs`')]), 1)
            self.assertTrue(all(len(rows) <= 2 for target, rows in pages))
            records = [row for target, rows in pages for row in rows]
            self.assertEqual(len(records), sum(map(len, self.documents.values())))
            self.assertTrue(all(row['source_file'].endswith('.jsonl') and row['source_record'] > 0 for row in records))
            workflow = [row for row in records if row['source_alias'] in bronze.WORKFLOW_TYPES]
            self.assertEqual(len(workflow), 4)
            self.assertTrue(all(row['source_collection'] == 'ai_email_workflows' and row['source_file'].endswith('email_workflows.jsonl') for row in workflow))

    def test_unknown_workflow_type_aborts_before_run_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            self.write_files(folder)
            with (folder/'email_workflows.jsonl').open('a', encoding='utf-8') as stream:
                stream.write('\n{"_id":"unexpected","record_type":"future"}')
            appends = []
            config = dict(self.config, jsonl_path=temp)
            with patch.object(bronze, 'ensure_schemas'), patch.object(bronze, 'assert_delta'), \
                 patch.object(bronze, 'ensure_bronze_table'), patch.object(bronze, 'merge_page'), \
                 patch.object(bronze, 'append_frame', side_effect=lambda s,r,t: appends.append(t)):
                with self.assertRaisesRegex(ValueError, 'record_type'):
                    bronze.ingest_jsonl(Spark(), config)
            self.assertFalse(any(t.endswith('`_runs`') for t in appends))

    def test_missing_file_and_partial_ingestion_never_publish_run(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            config = dict(self.config, jsonl_path=temp)
            with patch.object(bronze, 'ensure_schemas') as ensure:
                with self.assertRaisesRegex(ValueError, 'ausente'):
                    bronze.ingest_jsonl(Spark(), config)
                ensure.assert_not_called()
            self.write_files(folder)
            (folder/'email_dispatches.jsonl').write_text('invalid', encoding='utf-8')
            appends = []
            with patch.object(bronze, 'ensure_schemas'), patch.object(bronze, 'assert_delta'), \
                 patch.object(bronze, 'ensure_bronze_table'), patch.object(bronze, 'merge_page'), \
                 patch.object(bronze, 'append_frame', side_effect=lambda s,r,t: appends.append(t)):
                with self.assertRaises(ValueError):
                    bronze.ingest_jsonl(Spark(), config)
            self.assertFalse(any(t.endswith('`_runs`') for t in appends))

    def test_volume_path_and_checkpoint_identity(self):
        for value in ('/tmp/data', '/Volumes/c/s/v/../other', '/Volumes/c/s'):
            with self.assertRaises(ValueError):
                validate_jsonl_path(value)
        self.assertEqual(validate_jsonl_path('/Volumes/c/s/v/batch'), '/Volumes/c/s/v/batch')
        self.assertNotEqual(config_signature(self.config), config_signature(dict(self.config, jsonl_path='/Volumes/c/s/v/other')))

    def test_serverless_job_and_no_unsupported_spark_calls(self):
        import yaml
        job = yaml.safe_load((ROOT/'databricks.yml').read_text())['resources']['jobs']['argos_ai_medallion']
        settings = json.loads((ROOT/'docs/job-settings.json').read_text())
        for tasks in (job['tasks'], settings['tasks']):
            for task in tasks:
                self.assertFalse({'existing_cluster_id','new_cluster','job_cluster_key'} & task.keys())
        blocked = {'persist','unpersist','cache','localCheckpoint','checkpoint','rdd','sparkContext','_jvm'}
        for path in (ROOT/'argos').glob('*.py'):
            tree = ast.parse(path.read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    self.assertNotIn(node.attr, blocked, path.name)
        self.assertEqual(DEFAULTS['catalog'], 'workspace')
        self.assertFalse({'source_mode','mongo_database','mongo_secret_scope','force_snapshot','lookback_minutes'} & DEFAULTS.keys())
        self.assertFalse(hasattr(bronze, 'connection_uri'))
        self.assertFalse(hasattr(bronze, 'page_query'))
        self.assertNotIn('Path.cwd()', (ROOT/'notebooks/02_silver.py').read_text())

if __name__ == '__main__':
    unittest.main()
