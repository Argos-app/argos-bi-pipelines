# Verificação da versão MongoDB

A Bronze usa PyMongo para ler snapshots completos. A suite local testa o cliente simulado, confirmação global de lote, collection ausente, falha de conexão e tipos de workflow desconhecidos. Os testes locais não executam Spark/Delta, rede ou o Job no Databricks. A primeira execução deve verificar o Mongo real, conexão, permissões e rede, sintaxe/execução Lakeflow, publicação e dashboards. Consulte [Configuração](CONFIGURACAO_DATABRICKS.md).
