"""Testes locais sem banco: BSON, contratos, paginação, SQL e DAG."""
import ast
import copy
import json
from pathlib import Path
import sys
import unittest
from datetime import timedelta
from decimal import Decimal

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from argos.bronze import canonical_json,bronze_record
from argos.common import DEFAULTS,sources,config_signature
from argos.parsing import parse_record
from argos.schemas import SCHEMAS,ddl
from sample_records import fixture_documents
from argos.gold import gold_queries,GOLD_KEYS

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.docs,self.ids=fixture_documents()

    def parse(self,alias,document):
        return parse_record(alias,canonical_json(document))

    def test_all_15_sources_have_valid_typed_fixtures(self):
        self.assertEqual(len(self.docs),15)
        for alias,documents in self.docs.items():
            for document in documents:
                with self.subTest(alias=alias): self.assertEqual(self.parse(alias,document)['errors'],[])

    def test_decimal128_and_unknown_payload_are_preserved(self):
        raw=self.docs['observations'][-1]
        raw['future_field']={'array':['CaseSensitive',{'nested':17}]}
        encoded=canonical_json(raw)
        self.assertIn('future_field',encoded)
        self.assertIn('$numberDecimal',encoded)
        parsed=self.parse('observations',raw)
        self.assertEqual(parsed['record']['billing']['reported_cost'],Decimal('0.00012345678901234567'))
        self.assertEqual(parsed['record']['model_requested'],'fixture-model')

    def test_unknown_usage_and_billing_stay_none(self):
        parsed=self.parse('observations',self.docs['observations'][0])
        self.assertIsNone(parsed['record']['usage'])
        self.assertIsNone(parsed['record']['billing'])

    def test_started_and_terminal_have_same_identity_but_distinct_versions(self):
        terminal=self.docs['observations'][0]
        started=copy.deepcopy(terminal)
        started.update(status='started',ended_at=None,duration_ms=None)
        self.assertEqual(self.parse('observations',started)['errors'],[])
        config=dict(DEFAULTS)
        source=sources(config)[4]
        a=bronze_record(started,source,config,'run',started['started_at'])
        b=bronze_record(terminal,source,config,'run',started['started_at'])
        self.assertEqual(a['document_id'],b['document_id'])
        self.assertNotEqual(a['version_key'],b['version_key'])
        self.assertEqual(a['version_key'],bronze_record(started,source,config,'run',started['started_at'])['version_key'])

    def test_timestamp_only_terminal_change_is_not_semantic_conflict(self):
        doc=copy.deepcopy(self.docs['observations'][0])
        first=self.parse('observations',doc)['semantic_hash']
        doc['updated_at']+=timedelta(seconds=1)
        self.assertEqual(first,self.parse('observations',doc)['semantic_hash'])
        doc['duration_ms']+=1
        self.assertNotEqual(first,self.parse('observations',doc)['semantic_hash'])

    def test_no_terminal_without_end(self):
        doc=copy.deepcopy(self.docs['observations'][0]); doc.pop('ended_at')
        self.assertIn('terminal:missing_end',self.parse('observations',doc)['errors'])

    def test_invalid_billing_not_coerced_to_zero(self):
        doc=copy.deepcopy(self.docs['observations'][-1]); doc['billing']['reported_cost']='invalid'
        result=self.parse('observations',doc)
        self.assertTrue(result['errors'])
        self.assertIsNone(result['record']['billing']['reported_cost'])

    def test_naive_timestamp_invalid(self):
        doc=copy.deepcopy(self.docs['observations'][0]); doc['started_at']='2026-10-19T12:00:00'
        self.assertTrue(self.parse('observations',doc)['errors'])

    def test_bool_is_not_token_count(self):
        doc=copy.deepcopy(self.docs['observations'][-1]); doc['usage']['input_tokens']=True
        self.assertTrue(self.parse('observations',doc)['errors'])

    def test_degraded_status_and_array_presence(self):
        doc=copy.deepcopy(self.docs['interactions'][0]); doc['status']='degraded'
        result=json.loads(doc['result_json']); result.update(status='degraded',degradation_reasons=['judge_unavailable'])
        doc['result_json']=json.dumps(result)
        parsed=self.parse('interactions',doc)
        self.assertEqual(parsed['errors'],[])
        self.assertEqual(parsed['record']['degradation_reasons'],['judge_unavailable'])

    def test_invalid_embedded_result_preserves_interaction(self):
        doc=copy.deepcopy(self.docs['interactions'][0]); doc['result_json']='{truncated'
        parsed=self.parse('interactions',doc)
        self.assertEqual(parsed['errors'],[])
        self.assertEqual(parsed['record']['result_parse_status'],'invalid')
        self.assertIsNone(parsed['record']['response_kind'])

    def test_answer_link_comes_from_message_payload(self):
        parsed=self.parse('messages',self.docs['messages'][1])
        self.assertEqual(parsed['record']['source_interaction_id'],self.ids['root'])
        self.assertEqual(parsed['record']['answered_question_id'],self.ids['question'])

    def test_truncated_judge_not_rejected_or_approved(self):
        doc=copy.deepcopy(self.docs['execution_events'][0]); doc['sanitized_output']='{"decision":"approved"...'
        parsed=self.parse('execution_events',doc)
        self.assertEqual(parsed['errors'],[])
        self.assertEqual(parsed['record']['judge_parse_status'],'invalid')
        self.assertIsNone(parsed['record']['judge_decision'])

    def test_array_none_empty_and_invalid_null_element(self):
        doc=copy.deepcopy(self.docs['observations'][0])
        self.assertIsNone(self.parse('observations',doc)['record']['resources'])
        doc['resources']=[]
        self.assertEqual(self.parse('observations',doc)['record']['resources'],[])
        doc['resources']=[None]
        self.assertTrue(self.parse('observations',doc)['errors'])

    def test_version_one_legacy_ids_optional(self):
        doc=copy.deepcopy(self.docs['observations'][0]); doc['schema_version']='1'
        for key in ('interaction_id','session_id','correlation_id','parent_observation_id'): doc.pop(key,None)
        self.assertEqual(self.parse('observations',doc)['errors'],[])

    def test_snapshot_sources_and_bronze_lineage(self):
        self.assertTrue(all(source['mode']=='snapshot' for source in sources(DEFAULTS)))
        source=sources(DEFAULTS)[0]
        row=bronze_record(self.docs['sessions'][0],source,DEFAULTS,'run',self.docs['sessions'][0]['created_at'], '/Volumes/c/s/v/sessions.jsonl', 3)
        self.assertEqual(row['source_record'],3)
        self.assertTrue(row['source_file'].endswith('sessions.jsonl'))

    def test_invalid_collection_config_fails(self):
        config=dict(DEFAULTS,collections_json='{"unknown":{"enabled":false}}')
        with self.assertRaises(ValueError): sources(config)
        config=dict(DEFAULTS,collections_json='{"observations":{"enabled":"false"}}')
        with self.assertRaises(ValueError): sources(config)

    def test_source_override_changes_checkpoint_identity(self):
        a=dict(DEFAULTS); b=dict(DEFAULTS,collections_json='{"observations":{"collection":"custom"}}')
        self.assertNotEqual(config_signature(a),config_signature(b))

    def test_structure_matches_relational_convention(self):
        self.assertEqual(DEFAULTS['source_database'],'argos_ai')
        self.assertTrue((ROOT/'argos/delta_layers.py').is_file())
        self.assertTrue((ROOT/'docs/CONFIGURACAO_DATABRICKS.md').is_file())
        self.assertTrue((ROOT/'docs/VERIFICACAO.md').is_file())
        self.assertFalse((ROOT/'argos_ai').exists())
        self.assertFalse((ROOT/'copiar_colar').exists())
        self.assertFalse((ROOT/'pacotes').exists())
        self.assertFalse(list((ROOT/'notebooks').rglob('*.ipynb')))
        from argos.delta_layers import run_silver,run_gold,validate_pipeline
        self.assertTrue(all(callable(f) for f in (run_silver,run_gold,validate_pipeline)))

    def test_all_sources_have_schema(self):
        for source in sources(DEFAULTS):
            alias='observations' if source['alias']=='a2a_observations' else source['alias']
            self.assertIn(alias,SCHEMAS)

    def test_python_and_notebook_cells_compile(self):
        for path in ROOT.rglob('*.py'):
            ast.parse(path.read_text(encoding='utf-8'),filename=str(path))
        for path in ROOT.rglob('*.ipynb'):
            notebook=json.loads(path.read_text(encoding='utf-8'))
            for cell in notebook['cells']:
                code=''.join(cell['source'])
                if not code.lstrip().startswith('%'): ast.parse(code)
            if path.name in ('01_bronze.ipynb','02_silver.ipynb'):
                self.assertTrue(''.join(notebook['cells'][0]['source']).startswith('%pip'))

    def test_gold_only_selects_and_joins_and_dashboards_only_select(self):
        import sqlglot
        from sqlglot import exp
        config=dict(DEFAULTS)
        for name,statement in gold_queries(config).items():
            tree=sqlglot.parse_one(statement,read='databricks')
            self.assertIsInstance(tree,exp.Select)
            self.assertFalse(any(tree.find_all(exp.AggFunc,exp.Window,exp.Case,exp.Subquery)))
            self.assertNotIn('explode',statement.lower())
        q=(ROOT/'docs/queries-dashboard.sql').read_text(encoding='utf-8')
        statements=sqlglot.parse(q,read='databricks')
        self.assertEqual(len(statements),19)
        self.assertTrue(all(isinstance(s,exp.Select) for s in statements))
        self.assertIn('array_contains',q)
        self.assertEqual(len(list((ROOT/'dashboards').glob('*.sql'))),19)
        for spec in SCHEMAS.values():

            sqlglot.parse_one('CREATE TABLE t (r '+ddl(spec)+') USING DELTA',read='databricks')

    def test_job_dag_and_notebook_paths(self):
        import yaml
        bundle=yaml.safe_load((ROOT/'databricks.yml').read_text())
        job=bundle['resources']['jobs']['argos_ai_medallion']
        self.assertEqual(job['max_concurrent_runs'],1)
        keys={x['task_key'] for x in job['tasks']}
        self.assertEqual(len(keys),7)
        for task in job['tasks']:
            self.assertTrue((ROOT/task['notebook_task']['notebook_path']).exists())
            for dep in task.get('depends_on',[]): self.assertIn(dep['task_key'],keys)
        validation=next(x for x in job['tasks'] if x['task_key']=='validar')
        self.assertEqual({x['task_key'] for x in validation['depends_on']},set(GOLD_KEYS))

if __name__=='__main__': unittest.main(verbosity=2)
