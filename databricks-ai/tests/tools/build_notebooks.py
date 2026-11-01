"""Gera notebooks modulares e autocontidos a partir da implementação única."""
from pathlib import Path
import json
import re
import shutil

ROOT=Path(__file__).resolve().parents[2]
NAMES=('g_interactions','g_observations','g_invocations','g_email_dispatches')
HEADER='# Databricks notebook source\n'
SEP='\n# COMMAND ----------\n'
BOOT='''import sys
from pathlib import Path
try:
    project_root = dbutils.widgets.get('project_root')
except Exception:
    dbutils.widgets.text('project_root', '/Workspace/Shared/argos-ai-bi')
    project_root = dbutils.widgets.get('project_root')
root = Path(project_root)
if not root.is_absolute() or not (root / 'argos').is_dir():
    raise RuntimeError('Configure project_root com a raiz absoluta do projeto no Workspace ou use os notebooks autocontidos.')
sys.path.insert(0, str(root))
from argos.common import read_config
'''
PIP='# MAGIC %pip install pymongo==4.19.0\n'
BRONZE_ENTRY='''# Snapshot JSONL completo em jsonl_path (/Volumes).
config=read_config(dbutils)
ingest(spark,config)
'''

def inline(modules):
    blocks=[]
    for module in modules:
        text=(ROOT/'argos'/f'{module}.py').read_text(encoding='utf-8')
        text=re.sub(r'^from \.[^\n]*\(\s*\n(?:[^\n]*\n)*?\s*\)\s*\n|^from \.[^\n]+\n','',text,flags=re.MULTILINE)
        blocks.append(text)
    return SEP.join(blocks)

def write_notebook(relative, modular, standalone):
    for folder,body in (('notebooks',modular),('docs/entregaveis/copiar_colar',standalone)):
        path=ROOT/folder/relative
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(HEADER+body,encoding='utf-8')

write_notebook('01_bronze.py',PIP+SEP+BOOT+'from argos.bronze import ingest\n'+SEP+BRONZE_ENTRY,
    PIP+SEP+inline(('common','schemas','bronze'))+SEP+BRONZE_ENTRY)
entry='config=read_config(dbutils)\nrun_silver(spark,config)\n'
write_notebook('02_silver.py',PIP+SEP+BOOT+'from argos.delta_layers import run_silver\n'+SEP+entry,
    PIP+SEP+inline(('common','schemas','parsing','bronze','staging','silver'))+SEP+entry)
for name in NAMES:
    entry=f"config=read_config(dbutils)\nrun_gold(spark,config,'{name}')\n"
    write_notebook(f'gold/{name}.py',BOOT+'from argos.delta_layers import run_gold\n'+SEP+entry,
        inline(('common','gold'))+SEP+entry)
entry='config=read_config(dbutils)\nvalidate_pipeline(spark,config)\n'
write_notebook('04_validacao.py',BOOT+'from argos.delta_layers import validate_pipeline\n'+SEP+entry,
    inline(('common','gold'))+SEP+entry)

for folder in ('notebooks','docs/entregaveis/copiar_colar'):
    for path in (ROOT/folder).rglob('*.py'):
        code=path.read_text(encoding='utf-8').replace(HEADER,'',1)
        cells=[]
        for block in code.split('# COMMAND ----------'):
            lines=block.strip().splitlines(keepends=True)
            if not lines: continue
            lines=[line.replace('# MAGIC ','',1) if line.startswith('# MAGIC ') else line for line in lines]
            cells.append({'cell_type':'code','execution_count':None,'metadata':{},'outputs':[],'source':lines})
        notebook={'cells':cells,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'},
            'language_info':{'name':'python','version':'3.12'}},'nbformat':4,'nbformat_minor':5}
        output=(ROOT/'docs/entregaveis/notebooks_modulares'/path.relative_to(ROOT/'notebooks')) if folder=='notebooks' else path
        output.parent.mkdir(parents=True,exist_ok=True)
        output.with_suffix('.ipynb').write_text(json.dumps(notebook,ensure_ascii=False,indent=1)+'\n',encoding='utf-8')


import sys
sys.path.insert(0, str(ROOT))
from argos.common import DEFAULTS
parameters=[{'name':k,'default':v} for k,v in DEFAULTS.items() if k != 'environment']
tasks=[]
def task(key,path,dependencies=()):
    item={'task_key':key,
          'notebook_task':{'notebook_path':'/Workspace/Shared/argos-ai-bi/notebooks/'+path}}
    if dependencies: item['depends_on']=[{'task_key':x} for x in dependencies]
    tasks.append(item)
task('bronze','01_bronze',())
task('silver','02_silver',('bronze',))
for name in NAMES: task(name,'gold/'+name,('silver',))
task('validar','04_validacao',NAMES)
job={'name':'argos-ai-bi','max_concurrent_runs':1,'parameters':parameters,'tasks':tasks}
(ROOT/'docs/job-settings.json').write_text(json.dumps(job,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
yaml='''bundle:
  name: argos-ai-bi
resources:
  jobs:
    argos_ai_medallion:
      name: argos-ai-bi
      max_concurrent_runs: 1
      parameters:
'''
for p in parameters:
    yaml+='        - name: '+p['name']+'\n          default: '+json.dumps('${workspace.file_path}' if p['name']=='project_root' else p['default'])+'\n'
yaml+='      tasks:\n'
for item in tasks:
    yaml+='        - task_key: '+item['task_key']+'\n'
    if 'depends_on' in item:
        yaml+='          depends_on:\n'+''.join('            - task_key: '+d['task_key']+'\n' for d in item['depends_on'])
    path=item['notebook_task']['notebook_path'].split('/notebooks/')[1]+'.py'
    yaml+='          notebook_task:\n            notebook_path: ./notebooks/'+path+'\n'
yaml+='targets:\n  dev:\n    default: true\n    mode: development\n'
(ROOT/'databricks.yml').write_text(yaml,encoding='utf-8')
(ROOT/'argos/requirements.txt').write_text('pymongo==4.19.0\n',encoding='utf-8')
dashboard=ROOT/'docs'/'queries-dashboard.sql'
dashboard.parent.mkdir(exist_ok=True)

names=['uso_01_uso_semanal','custos_02_tokens_custos','operacional_03_latencia_etapas','operacional_04_latencia_interagentes',
       'operacional_05_indice_erros','operacional_06_throughput_agentes','custos_07_custo_interacao','custos_08_custo_resolucao',
       'operacional_08b_cobertura_ciclos','custos_09_projecao_custos','operacional_10_confiabilidade_http','operacional_11_uso_ferramentas',
       'operacional_12_recorrencia_falhas','operacional_13a_degradacao','qualidade_13b_qualidade_juiz','email_14_email','operacional_15_cobertura_observabilidade',
       'custos_16_roi','operacional_17_distribuicao_latencia']
parts=re.split(r'(?=^-- Q\d+[ab]?\.)',dashboard.read_text(encoding='utf-8'),flags=re.MULTILINE)
assert len(parts)==len(names)+1
for name,block in zip(names,parts[1:]):
    path = ROOT/'dashboards'/f'{name}.sql'
    content = parts[0]+re.sub(r'^-- Q\d+[ab]?\.[^\n]*\n', '', block)
    if not path.exists() or path.read_text(encoding='utf-8').rstrip() != content.rstrip():
        path.write_text(content,encoding='utf-8')
print('7 notebooks modulares + 7 autocontidos (.py e .ipynb); Job JSON/YAML e SQL atualizados.')
