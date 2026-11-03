"""Checagens locais do grafo Lakeflow; não substituem execução Spark."""
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch
import sqlglot
import yaml

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

class PipelineTests(unittest.TestCase):
    def load_pipeline(self):
        registered={}
        dp=types.ModuleType('pyspark.pipelines')
        def materialized_view(name):
            def register(func): registered[name]=func; return func
            return register
        dp.materialized_view=materialized_view
        dp.expect_or_fail=lambda *args: lambda func: func
        pyspark=types.ModuleType('pyspark'); pyspark.pipelines=dp
        sql=types.ModuleType('pyspark.sql'); sql.functions=types.SimpleNamespace(); sql.Window=object()
        class Spark:
            conf=types.SimpleNamespace(get=lambda key, default: default)
            def sql(self, text): return text
        with patch.dict(sys.modules,{'pyspark':pyspark,'pyspark.pipelines':dp,'pyspark.sql':sql}):
            module=runpy.run_path(str(ROOT/'lakeflow/silver_gold.py'),init_globals={'spark':Spark()})
        return module,registered

    def test_pipeline_registers_all_sources_gold_and_quality(self):
        module,registered=self.load_pipeline()
        self.assertEqual(len([k for k in registered if '._parsed_' in k]),15)
        for target in ('s_cycle_membership','s_invocations','s_event_messages','s_observation_resources','s_linkage_issues','q_records','_quality'):
            self.assertIn('workspace.silver_ia.'+target,registered)
        for target in ('g_interactions','g_observations','g_invocations','g_email_dispatches'):
            self.assertIn('workspace.gold_ia.'+target,registered)

    def test_membership_is_bounded_recursive_query(self):
        module,_=self.load_pipeline()
        query=module['membership']()
        # sqlglot ainda não reconhece a cláusula Databricks MAX RECURSION LEVEL.
        sqlglot.parse_one(query.replace(' MAX RECURSION LEVEL 101',''),read='databricks')
        self.assertIn('MAX RECURSION LEVEL 101',query)
        self.assertIn("THEN 'ambiguous'",query)
        self.assertIn("THEN 'unknown'",query)

    def test_quality_checks_are_split_and_all_reach_gate(self):
        module, registered = self.load_pipeline()
        prefix = 'workspace.silver_ia.'
        checks = {name[len(prefix):] for name in registered if name.startswith(prefix + '_quality_')}
        expected = {'_quality_states', '_quality_events'}
        expected.update('_quality_unique_' + label for label, _, _ in module['unique_checks'])
        expected.update('_quality_consistency_' + label for label in
                        ('interactions', 'observations', 'execution_events', 'messages'))
        self.assertTrue(expected <= checks)
        visited = set()
        pending = list(module['quality_parts'])
        while pending:
            name = pending.pop()
            self.assertNotIn(name, visited)
            visited.add(name)
            if name.startswith('_quality_group_'):
                children = registered[prefix + name].__defaults__[0]
                self.assertLessEqual(len(children), 5)
                pending.extend(children)
        self.assertEqual(visited, checks)

    def test_dashboard_queries_guard_gold_with_publication_and_latest_run(self):
        for file in (ROOT/'dashboards').glob('*.sql'):
            sql=file.read_text(encoding='utf-8')
            sqlglot.parse(sql,read='databricks')
            if 'workspace.gold_ia.g_' in sql:
                self.assertIn('workspace.gold_ia._publication',sql,file.name)
                self.assertIn('workspace.bronze_ia._runs',sql,file.name)
                self.assertIn('g.published_batch_id=p.published_batch_id',sql,file.name)
        self.assertEqual(len(sqlglot.parse((ROOT/'docs/queries-dashboard.sql').read_text(encoding='utf-8'),read='databricks')),19)

    def test_job_invalidates_before_pipeline_and_validates_after(self):
        bundle=yaml.safe_load((ROOT/'databricks.yml').read_text(encoding='utf-8-sig'))
        tasks=bundle['resources']['jobs']['argos_ai_lakeflow']['tasks']
        self.assertEqual([t['task_key'] for t in tasks],['iniciar','bronze','silver_gold','validar'])
        for previous,task in zip(tasks,tasks[1:]):
            self.assertIn({'task_key':previous['task_key']},task['depends_on'])
        self.assertTrue(bundle['resources']['pipelines']['silver_gold']['serverless'])

if __name__=='__main__': unittest.main()
