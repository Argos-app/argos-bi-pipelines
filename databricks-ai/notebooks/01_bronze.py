# Databricks notebook source
# MAGIC %pip install pymongo==4.19.0

# COMMAND ----------
import sys
from pathlib import Path
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / 'argos').is_dir():
        sys.path.insert(0,str(candidate))
        break
else:
    raise RuntimeError('Preserve argos/ e notebooks/ na mesma raiz OU use copiar_colar/.')
from argos.common import read_config
from argos.bronze import ingest

# COMMAND ----------
# Conexão: preencha os widgets de Secret Scope OU esta URI.
# Não use mongodb://localhost:27017: localhost aqui é o compute do Databricks.
# Nunca publique o notebook depois de preencher credenciais diretamente.
MONGO_URI = 'PREENCHA_A_URI_MONGODB_OU_USE_SECRET_SCOPE'
# Vazio reutiliza a URI principal; preencha só se o A2A usa outro cluster.
MONGO_A2A_URI = ''
config=read_config(dbutils)
ingest(spark,dbutils,config,MONGO_URI,MONGO_A2A_URI)
