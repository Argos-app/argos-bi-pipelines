"""Resolução local da raiz usada pelo notebook Bronze."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
source=(ROOT/'notebooks/01_bronze.py').read_text(encoding='utf-8')
bootstrap=source[source.index('import sys\n'):source.index('from argos.common import')]

class RootDiscoveryTests(unittest.TestCase):
    def run_bootstrap(self, cwd, configured):
        widgets=SimpleNamespace(get=lambda key: configured)
        namespace={'dbutils':SimpleNamespace(widgets=widgets)}
        with patch.object(Path,'cwd',return_value=cwd), patch.object(sys,'path',sys.path.copy()):
            exec(bootstrap,namespace)
        return namespace['root']

    def test_discovers_parent_even_if_shared_default_is_wrong(self):
        with tempfile.TemporaryDirectory() as tmp:
            project=Path(tmp)/'project'; (project/'argos').mkdir(parents=True)
            notebook=project/'notebooks'; notebook.mkdir()
            self.assertEqual(self.run_bootstrap(notebook,'/Workspace/Shared/argos-ai-bi'),project)

    def test_configured_root_is_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp); project=base/'project'; (project/'argos').mkdir(parents=True)
            other=base/'other'; other.mkdir()
            self.assertEqual(self.run_bootstrap(other,str(project)),project)

    def test_missing_root_explains_folder_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'Pasta argos não encontrada'):
                self.run_bootstrap(Path(tmp),str(Path(tmp)/'missing'))

if __name__=='__main__': unittest.main()
