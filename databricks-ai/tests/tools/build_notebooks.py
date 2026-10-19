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
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        sys.path.insert(0,str(candidate))
        break
else:
    raise RuntimeError('Preserve argos/ e notebooks/ na mesma raiz OU use copiar_colar/.')
from argos.common import read_config
'''
PIP='# MAGIC %pip install pymongo==4.19.0\n'
BRONZE_ENTRY='''# Conexão: preencha os widgets de Secret Scope OU esta URI.
# Não use mongodb://localhost:27017: localhost aqui é o compute do Databricks.
# Nunca publique o notebook depois de preencher credenciais diretamente.
MONGO_URI = 'PREENCHA_A_URI_MONGODB_OU_USE_SECRET_SCOPE'
# Vazio reutiliza a URI principal; preencha só se o A2A usa outro cluster.
MONGO_A2A_URI = ''
config=read_config(dbutils)
ingest(spark,dbutils,config,MONGO_URI,MONGO_A2A_URI)
'''

def inline(modules):
    blocks=[]
    for module in modules:
        text=(ROOT/'argos'/f'{module}.py').read_text(encoding='utf-8')
        text=re.sub(r'^from \.[^\n]+\n','',text,flags=re.MULTILINE)
        blocks.append('# Implementação: argos/'+module+'.py\n'+text)
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
    PIP+SEP+inline(('common','schemas','parsing','bronze','silver'))+SEP+entry)
for name in NAMES:
    entry=f"config=read_config(dbutils)\nrun_gold(spark,config,'{name}')\n"
    write_notebook(f'gold/{name}.py',BOOT+'from argos.delta_layers import run_gold\n'+SEP+entry,
        inline(('common','gold'))+SEP+entry)
entry='config=read_config(dbutils)\nvalidate_pipeline(spark,config)\n'
write_notebook('04_validacao.py',BOOT+'from argos.delta_layers import validate_pipeline\n'+SEP+entry,
    inline(('common','gold'))+SEP+entry)
entry='config=read_config(dbutils)\nfixture_config=run_fixture(spark,config)\n'
write_notebook('05_testes_fixture.py',PIP+SEP+BOOT+'from argos.fixture import run_fixture\n'+SEP+entry,
    PIP+SEP+inline(('common','schemas','parsing','bronze','silver','gold','fixture'))+SEP+entry)

# .ipynb preserva células %pip ao importar/abrir, além dos .py de origem Databricks.
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

# Arquivo Jobs API pode ser importado sem CLI; bundle usa o mesmo DAG.
parameters=[{'name':k,'default':v} for k,v in {
    'catalog':'ai_bi','bronze_schema':'bronze','silver_schema':'silver','gold_schema':'gold',
    'environment':'local','mongo_database':'argos_ai','mongo_secret_scope':'','mongo_secret_key':'',
    'mongo_a2a_database':'','mongo_a2a_secret_scope':'','mongo_a2a_secret_key':'',
    'collections_json':'{}','force_snapshot':'false','lookback_minutes':'5','page_size':'500',
    'max_documents_per_source':'1000000','max_page_bytes':'16777216','max_cycle_depth':'100','fail_on_quarantine':'true',
}.items()]
tasks=[]
def task(key,path,dependencies=()):
    item={'task_key':key,'existing_cluster_id':'PREENCHA_CLUSTER_ID',
          'notebook_task':{'notebook_path':'/Workspace/Shared/argos-ai-bi/notebooks/'+path}}
    if dependencies: item['depends_on']=[{'task_key':x} for x in dependencies]
    tasks.append(item)
task('bronze','01_bronze',())
task('silver','02_silver',('bronze',))
for name in NAMES: task(name,'gold/'+name,('silver',))
task('validar','04_validacao',NAMES)
job={'name':'argos-ai-bronze-silver-gold','max_concurrent_runs':1,'parameters':parameters,'tasks':tasks}
(ROOT/'docs/job-settings.json').write_text(json.dumps(job,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
yaml='''bundle:
  name: argos-ai-bi
variables:
  cluster_id:
    description: ID de um compute CLASSIC dedicado, Databricks Runtime 16.4 LTS ou superior
resources:
  jobs:
    argos_ai_medallion:
      name: argos-ai-bronze-silver-gold
      max_concurrent_runs: 1
      parameters:
'''
for p in parameters:
    yaml+='        - name: '+p['name']+'\n          default: '+json.dumps(p['default'])+'\n'
yaml+='      tasks:\n'
for item in tasks:
    yaml+='        - task_key: '+item['task_key']+'\n          existing_cluster_id: ${var.cluster_id}\n'
    if 'depends_on' in item:
        yaml+='          depends_on:\n'+''.join('            - task_key: '+d['task_key']+'\n' for d in item['depends_on'])
    path=item['notebook_task']['notebook_path'].split('/notebooks/')[1]+'.py'
    yaml+='          notebook_task:\n            notebook_path: ./notebooks/'+path+'\n'
yaml+='targets:\n  dev:\n    default: true\n    mode: development\n'
(ROOT/'databricks.yml').write_text(yaml,encoding='utf-8')
(ROOT/'argos/requirements.txt').write_text('pymongo==4.19.0\n',encoding='utf-8')
dashboard=ROOT/'docs'/'queries-dashboard.sql'
dashboard.parent.mkdir(exist_ok=True)
# Um arquivo SQL por dataset, seguindo o padrao relacional.
names=['01_uso_semanal','02_tokens_custos','03_latencia_etapas','04_latencia_interagentes',
       '05_indice_erros','06_throughput_agentes','07_custo_interacao','08_custo_resolucao',
       '08b_cobertura_ciclos','09_projecao_custos','10_confiabilidade_http','11_uso_ferramentas',
       '12_recorrencia_falhas','13a_degradacao','13b_qualidade_juiz','14_email','15_cobertura_observabilidade']
parts=re.split(r'(?=^-- Q\d+[ab]?\.)',dashboard.read_text(encoding='utf-8'),flags=re.MULTILINE)
assert len(parts)==len(names)+1
for name,block in zip(names,parts[1:]):
    (ROOT/'dashboards'/f'{name}.sql').write_text(parts[0]+block,encoding='utf-8')
print('8 notebooks modulares + 8 autocontidos (.py e .ipynb); Job JSON/YAML e SQL atualizados.')
