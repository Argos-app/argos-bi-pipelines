# Argos BI de IA — Free Edition

Projeto exclusivo para Databricks Free Edition, na pasta `databricks-ai/`, seguindo a organização do projeto relacional.

Entrada: snapshots completos JSONL em Volumes Unity Catalog. Execução: Job Serverless com sete tasks. Bronze → Silver → quatro Gold → validação. Gold somente seleciona e une entidades Silver; métricas ficam nos 19 SQLs de dashboard.

- [Configuração e execução](docs/CONFIGURACAO_DATABRICKS.md)
- [Arquitetura e evidências da IA](docs/plano-databricks-ia-free.md)
- [Validação e limites](docs/VERIFICACAO.md)
- [Gold e dependências](notebooks/gold/README.md)
- [Notebooks para importar](docs/entregaveis/copiar_colar/)
- [Pacotes Free Edition](docs/entregaveis/pacotes/)

Os pacotes `argos-ai-bi-importacao.zip` e `argos-ai-bi-completo.zip` são dois formatos da mesma versão Free: notebooks autocontidos para importar, ou projeto completo com módulos e testes. Não há uma versão alternativa de compute ou conexão direta com o MongoDB.

Defaults: `workspace.{bronze_ia,silver_ia,gold_ia}`. Para notebooks modulares, `project_root=/Workspace/Shared/argos-ai-bi`; para autocontidos, não é necessário importar módulos. Os notebooks usam PyMongo apenas para ler BSON/Extended JSON, sem acessar o banco.
