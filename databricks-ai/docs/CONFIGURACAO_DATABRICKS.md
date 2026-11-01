# Argos BI de IA — configurar a Free Edition

Versão 1.2.0, exclusivamente Serverless. A ingestão usa snapshots JSONL completos em Volumes Unity Catalog. O Job tem sete tasks Notebook; Gold permanece com seleção e junções e os indicadores ficam nos SQLs de dashboard.

## 2. Preparar arquivos de origem

Exporte snapshots completos das collections atuais em Extended JSON canônico.

Para dados reais, exporte cada collection com `mongoexport --jsonFormat=canonical`,
um documento por linha, e nomeie os arquivos conforme a lista abaixo. Não use `--jsonArray`. Os nomes esperados são:

```text
sessions.jsonl
interactions.jsonl
messages.jsonl
execution_events.jsonl
observations.jsonl
a2a_observations.jsonl
session_summaries.jsonl
user_memory.jsonl
report_artifacts.jsonl
email_workflows.jsonl
email_reports.jsonl
email_dispatches.jsonl
```

Cada arquivo é um **snapshot completo da collection**, inclusive quando vazio.
Arquivo ausente falha; arquivo vazio publica collection vazia. Não carregue apenas
novidades/um recorte temporal: o snapshot substitui o estado corrente na Silver.
Os nomes físicos e databases da origem continuam configuráveis por
`source_database`, `source_a2a_database` e `collections_json`, preservados como
metadados. O arquivo e a linha são registrados em `source_file`/`source_record`.

## 3. Criar o Volume e fazer upload

No SQL Editor, selecionando um warehouse disponível:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.bronze_ia;
CREATE VOLUME IF NOT EXISTS workspace.bronze_ia.landing;
```

Em um notebook conectado a Serverless, crie a pasta de entrada:

```python
dbutils.fs.mkdirs('/Volumes/workspace/bronze_ia/landing/atual')
```

Em Catalog → workspace → bronze_ia → Volumes → landing, faça upload dos 12
arquivos para a pasta `atual`. Confira:

```python
display(dbutils.fs.ls('/Volumes/workspace/bronze_ia/landing/atual'))
```

Para novos lotes, prefira outra pasta imutável (`lote_002`, por exemplo) e atualize
`jsonl_path` no Job. Não altere arquivos enquanto uma execução estiver ativa.
Os arquivos precisam permanecer estáveis durante toda a Bronze. Leitura de
collections/arquivos diferentes não constitui uma transação única da origem.

## 4. Importar os notebooks

Importe os arquivos `.ipynb` de `docs/entregaveis/copiar_colar/`
em `/Workspace/Shared/argos-ai-bi/notebooks/`, preservando a subpasta `gold`.
Os notebooks são autocontidos e evitam dependência do diretório de trabalho.

Alternativa modular: importe `argos/` como arquivos e `notebooks/` como notebooks.
Informe `project_root` com a raiz absoluta que contém `argos/`. O padrão é
`/Workspace/Shared/argos-ai-bi`; no bundle ele é preenchido com
`${workspace.file_path}`. Não misture os módulos com o projeto relacional.

Bronze e Silver têm `%pip install pymongo==4.19.0`. O pacote fornece
`bson`, necessário também no modo JSONL para preservar Date/Decimal128/UUIDs.
Não instale um pacote `bson` separado.

## 5. Criar o Job serverless

Em Jobs & Pipelines → Create → Job, crie `argos-ai-bronze-silver-gold`.
Todas as atividades são Notebook, Source Workspace, Compute **Serverless**.

| Task | Notebook em notebooks/ | Depends on |
|---|---|---|
| bronze | 01_bronze | nenhuma |
| silver | 02_silver | bronze |
| g_interactions | gold/g_interactions | silver |
| g_observations | gold/g_observations | silver |
| g_invocations | gold/g_invocations | silver |
| g_email_dispatches | gold/g_email_dispatches | silver |
| validar | 04_validacao | as quatro Gold |

Use All succeeded e Maximum concurrent runs = 1. A maior etapa paralela tem quatro
tasks. A Free limita cinco tasks simultâneas por conta: agende este Job sem
sobreposição com o Job relacional. Os limites de uso/tempo da conta continuam
aplicáveis; comece com execução manual e uma massa pequena.

Parâmetros principais (já estão nos defaults do JSON/YAML):

```text
catalog = workspace
bronze_schema = bronze_ia
silver_schema = silver_ia
gold_schema = gold_ia
jsonl_path = /Volumes/workspace/bronze_ia/landing/atual
project_root = /Workspace/Shared/argos-ai-bi
source_database = argos_ai
collections_json = {}
page_size = 500
fail_on_quarantine = true
```

A collection `ai_email_workflows` é exportada uma vez como `email_workflows.jsonl`; o pipeline separa os quatro tipos pelo campo `record_type` e rejeita tipos desconhecidos. A identidade interna da ingestão é fixa. A observação pode conter seu próprio campo `environment`, como dado da origem, sem seleção no Job. Use schemas novos se já houver dados de uma versão anterior.

A ingestão sempre faz snapshot completo. Todos os notebooks devem receber a mesma configuração de origem.

O bundle e `docs/job-settings.json` não contêm `existing_cluster_id`,
`job_cluster_key` ou `new_cluster`. Tasks Notebook sem configuração de cluster
usam serverless, conforme [exemplos oficiais](https://docs.databricks.com/aws/en/dev-tools/bundles/examples#job-that-uses-serverless-compute).
Para bundle, execute `databricks bundle validate` e `databricks bundle deploy`,
sem `--var cluster_id=...`.

## 6. Executar e verificar

Execute o Job completo. A publicação só é liberada após a validação:

```sql
SELECT * FROM workspace.gold_ia._publication;
SELECT * FROM workspace.silver_ia.q_records LIMIT 20;
SELECT COUNT(*) FROM workspace.gold_ia.g_interactions;
SELECT COUNT(*) FROM workspace.gold_ia.g_observations;
```

Os 19 SQLs de dashboard já usam `workspace.gold_ia`. Se mudar o catálogo
ou schema Gold, ajuste o prefixo dos SQLs. Não crie tabelas/views com os indicadores.

## 7. O que mudou no processamento

- Parsing e propagação iterativa dos vínculos usam tabelas Delta temporárias de
  processamento `_work_<UUID>_<etapa>` na Silver, sem cache/checkpoint/RDD.
- Cada tabela de staging é imutável durante a execução; uma etapa não sobrescreve
  a tabela que ainda serve de entrada para outra etapa.
- O staging é removido ao sair da Silver, inclusive em falhas capturadas. Em
  interrupção abrupta, confirme que nenhum Job está ativo antes de remover tabelas
  `_work_*` remanescentes. Elas contêm dados restritos, como a Silver.
- A Bronze faz MERGE por SQL, usando uma view temporária de sessão por página,
  removida ao terminar; não depende do objeto JVM DeltaTable.
- `source_file` e `source_record` são adicionados por migração aditiva a tabelas
  Bronze anteriores. Os contratos funcionais originais permanecem dentro do JSON.
- O Job continua bloqueando publicação após ingestão incompleta ou erros de
  qualidade. Não há fallback automático para dados sintéticos.

## 8. Validação e limites desta entrega

Testes locais cobrem contratos, BSON, SQL, DAG, ausência de APIs incompatíveis,
staging e limpeza em falhas, JSONL com arquivos vazios/ausentes/inválidos, limites
de página e confirmação global somente após todas as origens.

Não há sessão autenticada deste workspace nesta entrega. A execução real de
Spark Connect/Delta, as permissões de Volume e as quotas precisam ser verificadas
na primeira execução do Job. A leitura do Mongo é feita fora do Databricks para exportar os arquivos, quando forem dados reais; a pipeline Free não abre conexão com o banco.

## Visualizações SRE

Use os datasets de uso, custos, latências, erros e resolução para cards e séries temporais. `custos_16_roi.sql` calcula ROI com retorno bruto e custo total informados para a mesma coorte, período e moeda; sem valores válidos retorna NULL. `operacional_17_distribuicao_latencia.sql` fornece observações individuais para histogramas e boxplots, separados por operação e nível; não some durações de spans. Compare KPIs com uma consulta do período anterior usando a mesma janela e filtros. A projeção de 100 e 1.000 usuários depende de tarifas e volumes fornecidos, não é uma medição. O cadastro relacional possui cidade e estado, mas não coordenadas; só use mapa com geocodificação confiável e aprovada. Filtros analíticos não substituem autorização no Unity Catalog.
