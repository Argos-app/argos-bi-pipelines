"""Consistência dos guias de configuração e seus links locais."""
import re
import sys
import unittest
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / '.test-deps')]
import yaml
from argos.gold import gold_queries
from test_medallion import CONTRACT


class DocumentationTests(unittest.TestCase):
    def test_fresh_install_has_no_obsolete_assets_or_migration_functions(self):
        for name in ('notebooks/00_migrar_materialized_views.py', 'notebooks/05_migrar_time_bronze.py',
                     'notebooks/03_gold.py', 'pipelines/02_silver_03_gold.py', 'legacy/02_silver_03_gold.py.txt'):
            self.assertFalse((ROOT / name).exists())
        import argos.delta_layers as layers
        self.assertFalse(hasattr(layers, 'migrate_materialized_views'))
        self.assertFalse(hasattr(layers, 'run_gold'))

    def test_local_markdown_links_resolve(self):
        files = [path for path in ROOT.rglob('*.md') if '.test-deps' not in path.parts and '.databricks' not in path.parts]
        if (ROOT.parent / 'README.md').is_file():
            files.append(ROOT.parent / 'README.md')
        for path in files:
            text = path.read_text(encoding='utf-8')
            for match in re.finditer(r'(?<!!)\[[^\]]+\]\(([^\s)]+)\)', text):
                target = match[1].strip('<>')
                if target.startswith(('http:', 'https:', 'mailto:', '#')):
                    continue
                target = unquote(target.split('#', 1)[0])
                with self.subTest(document=str(path), target=target):
                    self.assertTrue((path.parent / target).exists())

    def test_documented_reference_tasks_match_job_paths_and_dependencies(self):
        guide = (ROOT / 'docs/CONFIGURACAO_DATABRICKS.md').read_text(encoding='utf-8')
        section = guide.split('<!-- TASK_MATRIX_START -->')[1].split('<!-- TASK_MATRIX_END -->')[0]
        rows = {}
        for line in section.splitlines():
            cells = [value.strip() for value in line.strip('|').split('|')]
            if len(cells) == 4 and cells[0].startswith('`'):
                rows[cells[0].strip('`')] = cells
        bundle = yaml.safe_load((ROOT / 'databricks.yml').read_text(encoding='utf-8'))
        self.assertEqual(set(bundle['resources']['jobs']), {'argos_medallion'})
        tasks = {task['task_key']: task for task in bundle['resources']['jobs']['argos_medallion']['tasks']}
        reference = {'bronze', 'silver', 'validar'} | {'gold_' + name for name in gold_queries('argos', 'silver', 'public', set(CONTRACT))}
        self.assertTrue(reference <= set(rows))
        for name, cells in rows.items():
            with self.subTest(task=name):
                self.assertIn(name, tasks)
                self.assertEqual(cells[1].strip('`'), tasks[name]['notebook_task']['notebook_path'])
                listed_dependencies = set(re.findall(r'`([^`]+)`', cells[2]))
                configured_dependencies = {value['task_key'] for value in tasks[name].get('depends_on', [])}
                self.assertEqual(listed_dependencies, configured_dependencies)
                self.assertEqual(cells[3].strip('`'), tasks[name].get('run_if', 'ALL_SUCCESS'))

    def test_gold_guide_covers_the_preserved_outputs_and_modular_extension(self):
        guide = (ROOT / 'notebooks/gold/README.md').read_text(encoding='utf-8')
        documented = set(re.findall(r'^\| `([^`]+)` \|', guide, re.M))
        reference = set(gold_queries('argos', 'silver', 'public', set(CONTRACT)))
        self.assertTrue(reference <= documented)
        self.assertTrue((ROOT / 'notebooks/gold/_template.py.txt').is_file())
        self.assertIn('GOLD_DEPENDENCIES', guide)

    def test_report_distinguishes_local_checks_from_remote_execution(self):
        report = (ROOT / 'docs/VERIFICACAO.md').read_text(encoding='utf-8')
        self.assertIn('Não executado', report)
        self.assertIn('JDBC', report)
        self.assertIn('DuckDB', report)
        guide = (ROOT / 'docs/CONFIGURACAO_DATABRICKS.md').read_text(encoding='utf-8')
        self.assertIn('CREATE', guide)
        self.assertIn('primeira', guide)
        self.assertIn('cinco tasks simultâneas', guide)


if __name__ == '__main__':
    unittest.main(verbosity=2)
