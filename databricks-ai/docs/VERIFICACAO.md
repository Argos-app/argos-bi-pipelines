# Verificação das pipelines de IA

Reorganização para o padrão relacional: `argos/`, `dashboards/`, `docs/`, `notebooks/`, `tests/` e `databricks.yml`.
Imports, ferramentas de geração, caminhos do Job e documentação atualizados. As pipelines relacionais não foram alteradas.

Testes locais: BSON/Decimal128, fixtures das 15 origens, tipos inválidos, paginação, SQL sem agregações na Gold, schemas, imports e DAG.
Resultado local: 23 testes aprovados após a reorganização. Os 69 arquivos relacionais verificados mantiveram hashes SHA-256 idênticos.

Pendências no ambiente do usuário: conexão MongoDB, rede, credenciais, compute classic dedicado, catálogo e execução Spark/Delta.
Use `notebooks/05_testes_fixture.py` para testar as transformações com dados sintéticos em schemas isolados no Databricks.
