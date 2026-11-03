# BI de IA — MongoDB e Lakeflow

A Bronze lê diretamente as collections MongoDB com PyMongo, grava snapshots completos em Delta e confirma o lote somente após todas as origens. O Job padrão segue `iniciar → bronze → silver_gold → validar`. Silver, Gold e dashboards consomem apenas lotes confirmados.

- [Configuração MongoDB e execução](docs/CONFIGURACAO_DATABRICKS.md)
- [Lakeflow, publicação e limitações](lakeflow/README.md)
- [SQLs dos dashboards](dashboards/)

Confira a configuração, os dados carregados e os dashboards em cada execução no Databricks.
