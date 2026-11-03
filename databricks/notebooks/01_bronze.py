# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC Bronze: SELECT * via JDBC; versões e snapshots em Delta append.


# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

import importlib
import sys
import tempfile
from pathlib import Path

from databricks.sdk import WorkspaceClient

# Caminho da pasta no Workspace, usado pela API do Databricks.
workspace_argos = "/Users/PREENCHA_USUARIO/argos-bi/argos"

# Cópia temporária no disco local da sessão.
local_root = Path(tempfile.mkdtemp(prefix="argos_"))
local_argos = local_root / "argos"
local_argos.mkdir()

client = WorkspaceClient()

for nome in ("__init__.py", "common.py", "bronze.py"):
    origem = f"{workspace_argos}/{nome}"

    with client.workspace.download(origem) as arquivo:
        conteudo = arquivo.read()

    (local_argos / nome).write_bytes(conteudo)

# Evita reutilizar uma versão anterior do pacote já carregada.
for nome in list(sys.modules):
    if nome == "argos" or nome.startswith("argos."):
        del sys.modules[nome]

sys.path.insert(0, str(local_root))
importlib.invalidate_caches()

import argos.common as common_module
import argos.bronze as bronze_module

ingest = bronze_module.ingest


# COMMAND ----------

jdbc_user = "PREENCHA_USUARIO_JDBC"
jdbc_password = "PREENCHA_SENHA_JDBC"

defaults = {
    "jdbc_url": (
        "PREENCHA_URL_JDBC"
    ),
    "catalog": "main",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "self_service",
    "source_schema": "public",
    "lookback_hours": "24",
    "force_snapshot": "false",
}
for key, default in defaults.items():
    dbutils.widgets.text(key, default)

config = {key: dbutils.widgets.get(key) for key in defaults}
config.update(jdbc_user=jdbc_user, jdbc_password=jdbc_password)
ingest(spark, config)