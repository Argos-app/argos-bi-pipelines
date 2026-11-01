# Verificação — Free Edition 1.2.0

Projeto exclusivo para Serverless, renomeado para databricks-ai. A Bronze lê somente snapshots JSONL de Volumes. Removidos conexão direta, URI/secrets Mongo, modo de origem selecionável, lookback e force_snapshot. source_database/source_a2a_database são apenas metadados de proveniência.

Validação local: contratos BSON e 15 origens; SQL, schemas, notebooks e DAG; staging Delta imutável e limpeza em falhas; arquivos vazios/ausentes/inválidos; limites de página e confirmação global. **32 testes locais passaram** após a limpeza e renomeação.

Não foi executado Spark Connect/Delta em workspace autenticado. Execute o Job com os 12 arquivos no Volume. Os filtros ambiente dos dashboards de IA continuam removidos.
