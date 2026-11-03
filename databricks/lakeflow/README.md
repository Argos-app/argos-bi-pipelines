# Pipeline Lakeflow relacional

A ingestão JDBC permanece no notebook `notebooks/01_bronze.py`. Execute-a antes da pipeline: ela faz a carga incremental, os snapshots e mantém `_ingestion_state` e `_table_manifest`. As definições em `relacional.py` criam materialized views para Silver e Gold a partir das tabelas Bronze.

O contrato é fixado nas 30 tabelas e colunas da versão atual de `tests/schema_contract.json`. Mudanças na origem exigem atualizar a definição. Uma tabela ou coluna necessária ausente faz a atualização falhar. Tabelas extras não são publicadas automaticamente.

## Criar no Databricks

1. Importe `lakeflow/relacional.py` como notebook e `argos/` e `notebooks/01_bronze.py` na mesma raiz do projeto.
2. Crie os schemas `silver` e `self_service` no catálogo escolhido, ou permita que a Bronze os crie. Como o workspace ainda não contém dados, a pipeline pode criar as materialized views nesses schemas diretamente.
3. Em **Jobs & Pipelines**, crie uma pipeline Lakeflow com o notebook `relacional.py` como código-fonte, publicação Unity Catalog padrão e compute compatível com materialized views.
4. Na configuração da pipeline, defina `argos.catalog`, `argos.source_schema`, `argos.bronze_schema`, `argos.silver_schema` e `argos.gold_schema`. Os padrões são `main`, `public`, `bronze`, `silver` e `self_service`, respectivamente.
5. Crie um job com uma tarefa Notebook executando `01_bronze`, seguida de uma tarefa Pipeline que atualiza a pipeline criada e de uma tarefa Notebook `lakeflow/validar.py`, com os mesmos parâmetros de catálogo e schemas. Configure na Bronze o mesmo catálogo, schema de origem e Bronze, além de conexão JDBC e credenciais PostgreSQL. Configure `max_concurrent_runs=1` e não execute outra ingestão ao mesmo tempo.
6. Na primeira execução, aguarde a Bronze concluir antes da primeira atualização da pipeline. Ajuste o catálogo/schema das consultas de dashboard para o schema Gold escolhido.

O notebook `lakeflow/validar.py` verifica chaves Silver e cardinalidade dos fatos Gold após a pipeline. O `04_validacao.py` antigo exige tabelas Delta comuns e não é compatível com essas materialized views. Acompanhe o event log e compare contagens e amostras com o PostgreSQL de origem. A execução da pipeline não substitui a conferência de permissões, driver, dados, dependências e dashboards no workspace.
