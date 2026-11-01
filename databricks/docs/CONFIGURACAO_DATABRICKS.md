# Começar do zero no Databricks, pela interface

Esta instalação parte de **nenhum schema e nenhuma tabela de destino**. Use um único Job com 38 atividades Notebook: Bronze, Silver, 35 Gold e validação. A Gold executa em sete lotes de até cinco tasks. Não há necessidade de CLI ou deploy para configurar pela interface.

## 1. Escolher o catálogo

Abra **Catalog** e escolha um catálogo que já exista e no qual você possa criar schemas. Por exemplo, use `workspace`, se estiver disponível e você tiver permissão. O default dos notebooks é `main`; informe `workspace` nos parâmetros do Job quando esse for seu catálogo. O projeto cria schemas e tabelas, mas não cria um catálogo nem o banco PostgreSQL de origem.

## 2. Importar os arquivos para o Workspace

Crie `/Workspace/Users/SEU_USUARIO/argos-projeto/`. Importe os módulos de `argos/` como arquivos Python e os `.py` de `notebooks/` como notebooks Databricks. Preserve esta organização:

```text
argos-projeto/
├── argos/
│   ├── __init__.py
│   ├── common.py
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── delta_layers.py
├── notebooks/
│   ├── 01_bronze
│   ├── 02_silver
│   ├── 04_validacao
│   └── gold/                 35 notebooks; um por tabela/contexto
└── dashboards/               consultas SQL opcionais para os datasets
```

A origem local é a pasta `databricks` deste repositório. Não é necessário importar `tests/`, `.test-deps/` ou `.databricks/`. O arquivo `__init__.py` deve manter esse nome. O `databricks.yml` documenta a configuração automatizada equivalente; não é um notebook.

## 3. Preencher a conexão

Abra `notebooks/01_bronze` e preencha `jdbc_password`. Confira `jdbc_user` e `jdbc_url`. A URL deve começar por `jdbc:postgresql://`, sem credenciais embutidas. As credenciais ficam diretamente no notebook, conforme solicitado; não há secret scope.

TIME/TIMETZ da origem já é convertido para texto no próprio SELECT JDBC, antes da primeira gravação Delta. Datas e timestamps permanecem com seus tipos. Não é necessário executar nenhum notebook adicional para isso.

## 4. Criar o Job

Entre em **Jobs & Pipelines → Create → Job** (ou no cartão **Job** da página). Escolha **Notebook** e configure a primeira task:

| Campo | Valor |
|---|---|
| Task name | `bronze` |
| Source | `Workspace` |
| Path | Selecione `argos-projeto/notebooks/01_bronze` |
| Compute | `Serverless`, se solicitado |
| Depends on | Nenhuma |

Clique em **Save task** e nomeie o Job `argos-bronze-silver-self-service`. O caminho de criação é descrito na [documentação oficial](https://docs.databricks.com/aws/en/jobs/configure-job).

## 5. Parâmetros do Job

No painel **Job details → Edit parameters**, configure:

| Parâmetro | Exemplo |
|---|---|
| `catalog` | `workspace` (ou seu catálogo existente) |
| `source_schema` | `public` |
| `bronze_schema` | `bronze` |
| `silver_schema` | `silver` |
| `gold_schema` | `self_service` |
| `lookback_hours` | `24` |
| `force_snapshot` | `false` |

Esses valores são repassados às tasks Notebook. Confira se não há valores divergentes nos widgets de uma execução manual. A senha fica apenas no notebook Bronze. [Parâmetros de Job](https://docs.databricks.com/aws/en/jobs/job-parameters).

Na primeira execução, a Bronze lê toda a origem porque não existem destinos nem watermark, mesmo com `force_snapshot=false`. Ela executa `CREATE SCHEMA IF NOT EXISTS` para os três schemas e cria `_table_manifest`, `_ingestion_state` e as tabelas Bronze. A Silver e cada Gold criam suas tabelas na sequência. Nas execuções seguintes, o Job continua funcionando: Bronze append/incremental ou snapshot conforme a tabela; Silver e Gold sobrescrevem o estado atual em Delta. `force_snapshot=true` é uma opção de reconciliação completa nas execuções futuras, não um requisito de instalação.

## 6. Adicionar Silver, Gold e validação

Na aba **Tasks**, clique em **Add task → Notebook**. Configure `silver` apontando para `02_silver`, com **Depends on = bronze** e **Run if dependencies = All succeeded**.

Adicione as 35 tasks Gold usando os nomes, os notebooks e todas as dependências da matriz abaixo. Cada Gold escreve uma única tabela. O nome da task tem prefixo `gold_`; o arquivo não tem esse prefixo (por exemplo, `gold_application` aponta para `gold/application`).

Por último, crie `validar`, apontando para `04_validacao`, dependendo de todas as 35 tasks Gold com **All succeeded**. Em **Job details → Advanced settings**, mantenha **Maximum concurrent runs = 1**. Não configure agendamento até concluir a primeira execução.

## 7. Lotes Gold e dependências

| Lote | Atividades | Aguarda |
|---|---|---|
| 1 | `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category` | `silver` |
| 2 | `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user` | Todas as cinco tasks do lote 1 |
| 3 | `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access` | Todas as cinco tasks do lote 2 |
| 4 | `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit` | Todas as cinco tasks do lote 3 |
| 5 | `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot` | Todas as cinco tasks do lote 4 |
| 6 | `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission` | Todas as cinco tasks do lote 5 |
| 7 | `gold_property`, `gold_stock_movement`, `gold_supplier`, `gold_therapeutic_category`, `gold_user_property` | Todas as cinco tasks do lote 6 |

Cada task aguarda todas as tasks do lote anterior, além de `silver` e dos contextos de negócio. No lote 1, fornecedor e categoria aguardam `_context_medicine`; por isso são até cinco simultâneas, não obrigatoriamente cinco. O lote seguinte só inicia quando o anterior inteiro termina com sucesso. Uma falha bloqueia os posteriores e a validação.

Os lotes de até cinco tasks são uma característica da configuração atual do job, não um limite imposto aqui à sua conta. `max_concurrent_runs=1` impede execuções simultâneas completas do mesmo job; ele não limita, por si só, o paralelismo entre suas tasks. O paralelismo efetivo também depende do compute e das configurações do workspace. Para aumentá-lo, revise as dependências no `databricks.yml` e preserve os vínculos de dados antes de implantar.

```text
bronze → silver → lote 1 → lote 2 → lote 3 → lote 4 → lote 5 → lote 6 → lote 7 → validar
```

## 8. Matriz exata para preencher Depends on

Os caminhos abaixo são relativos à raiz `argos-projeto`. Na interface, selecione o notebook correspondente no Workspace. `ALL_SUCCESS` aparece como **All succeeded**.

<!-- TASK_MATRIX_START -->
| Task | Notebook | Depende de | Run if |
|---|---|---|---|
| `bronze` | `./notebooks/01_bronze.py` | — | `ALL_SUCCESS` |
| `silver` | `./notebooks/02_silver.py` | `bronze` | `ALL_SUCCESS` |
| `gold__context_user_property` | `./notebooks/gold/_context_user_property.py` | `silver` | `ALL_SUCCESS` |
| `gold__context_medicine` | `./notebooks/gold/_context_medicine.py` | `silver` | `ALL_SUCCESS` |
| `gold__context_medicine_batch` | `./notebooks/gold/_context_medicine_batch.py` | `silver` | `ALL_SUCCESS` |
| `gold__context_supplier` | `./notebooks/gold/_context_supplier.py` | `silver`, `gold__context_medicine` | `ALL_SUCCESS` |
| `gold__context_therapeutic_category` | `./notebooks/gold/_context_therapeutic_category.py` | `silver`, `gold__context_medicine` | `ALL_SUCCESS` |
| `gold_access_audit` | `./notebooks/gold/access_audit.py` | `silver`, `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category` | `ALL_SUCCESS` |
| `gold_alert` | `./notebooks/gold/alert.py` | `silver`, `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category` | `ALL_SUCCESS` |
| `gold_animal_exception` | `./notebooks/gold/animal_exception.py` | `silver`, `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category` | `ALL_SUCCESS` |
| `gold_animal_lot_history` | `./notebooks/gold/animal_lot_history.py` | `silver`, `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category` | `ALL_SUCCESS` |
| `gold_app_user` | `./notebooks/gold/app_user.py` | `silver`, `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category` | `ALL_SUCCESS` |
| `gold_application` | `./notebooks/gold/application.py` | `silver`, `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user` | `ALL_SUCCESS` |
| `gold_application_animal` | `./notebooks/gold/application_animal.py` | `silver`, `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user` | `ALL_SUCCESS` |
| `gold_attachment` | `./notebooks/gold/attachment.py` | `silver`, `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user` | `ALL_SUCCESS` |
| `gold_change_audit` | `./notebooks/gold/change_audit.py` | `silver`, `gold__context_user_property`, `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user` | `ALL_SUCCESS` |
| `gold_daily_access` | `./notebooks/gold/daily_access.py` | `silver`, `gold__context_user_property`, `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user` | `ALL_SUCCESS` |
| `gold_data_catalog` | `./notebooks/gold/data_catalog.py` | `silver`, `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access` | `ALL_SUCCESS` |
| `gold_device` | `./notebooks/gold/device.py` | `silver`, `gold__context_user_property`, `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access` | `ALL_SUCCESS` |
| `gold_dim_date` | `./notebooks/gold/dim_date.py` | `silver`, `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access` | `ALL_SUCCESS` |
| `gold_herd` | `./notebooks/gold/herd.py` | `silver`, `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access` | `ALL_SUCCESS` |
| `gold_interaction_audit` | `./notebooks/gold/interaction_audit.py` | `silver`, `gold__context_user_property`, `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access` | `ALL_SUCCESS` |
| `gold_load_error` | `./notebooks/gold/load_error.py` | `silver`, `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit` | `ALL_SUCCESS` |
| `gold_load_mapping` | `./notebooks/gold/load_mapping.py` | `silver`, `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit` | `ALL_SUCCESS` |
| `gold_load_run` | `./notebooks/gold/load_run.py` | `silver`, `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit` | `ALL_SUCCESS` |
| `gold_load_staging` | `./notebooks/gold/load_staging.py` | `silver`, `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit` | `ALL_SUCCESS` |
| `gold_lot` | `./notebooks/gold/lot.py` | `silver`, `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit` | `ALL_SUCCESS` |
| `gold_medicine` | `./notebooks/gold/medicine.py` | `silver`, `gold__context_medicine`, `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot` | `ALL_SUCCESS` |
| `gold_medicine_batch` | `./notebooks/gold/medicine_batch.py` | `silver`, `gold__context_medicine_batch`, `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot` | `ALL_SUCCESS` |
| `gold_notification_delivery` | `./notebooks/gold/notification_delivery.py` | `silver`, `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot` | `ALL_SUCCESS` |
| `gold_occurrence` | `./notebooks/gold/occurrence.py` | `silver`, `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot` | `ALL_SUCCESS` |
| `gold_permission` | `./notebooks/gold/permission.py` | `silver`, `gold__context_user_property`, `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot` | `ALL_SUCCESS` |
| `gold_property` | `./notebooks/gold/property.py` | `silver`, `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission` | `ALL_SUCCESS` |
| `gold_stock_movement` | `./notebooks/gold/stock_movement.py` | `silver`, `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission` | `ALL_SUCCESS` |
| `gold_supplier` | `./notebooks/gold/supplier.py` | `silver`, `gold__context_supplier`, `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission` | `ALL_SUCCESS` |
| `gold_therapeutic_category` | `./notebooks/gold/therapeutic_category.py` | `silver`, `gold__context_therapeutic_category`, `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission` | `ALL_SUCCESS` |
| `gold_user_property` | `./notebooks/gold/user_property.py` | `silver`, `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission` | `ALL_SUCCESS` |
| `validar` | `./notebooks/04_validacao.py` | `gold__context_user_property`, `gold__context_medicine`, `gold__context_medicine_batch`, `gold__context_supplier`, `gold__context_therapeutic_category`, `gold_access_audit`, `gold_alert`, `gold_animal_exception`, `gold_animal_lot_history`, `gold_app_user`, `gold_application`, `gold_application_animal`, `gold_attachment`, `gold_change_audit`, `gold_daily_access`, `gold_data_catalog`, `gold_device`, `gold_dim_date`, `gold_herd`, `gold_interaction_audit`, `gold_load_error`, `gold_load_mapping`, `gold_load_run`, `gold_load_staging`, `gold_lot`, `gold_medicine`, `gold_medicine_batch`, `gold_notification_delivery`, `gold_occurrence`, `gold_permission`, `gold_property`, `gold_stock_movement`, `gold_supplier`, `gold_therapeutic_category`, `gold_user_property` | `ALL_SUCCESS` |
<!-- TASK_MATRIX_END -->

## 9. Primeira execução

Clique em **Run now** e acompanhe Bronze, Silver, os sete lotes Gold e `validar`. Não é necessário criar tabelas manualmente. A identidade do Job precisa conseguir criar schemas/tabelas no catálogo, conectar ao PostgreSQL e usar o driver `org.postgresql.Driver`.

A origem deve conter as tabelas de negócio usadas pelos notebooks. O contrato atual tem 30 tabelas, incluindo `dim_date`; não são fabricadas tabelas vazias para ocultar uma origem ausente. Se uma Gold informar entrada inexistente, confira a origem e o manifesto Bronze.

No SQL Editor, após o sucesso, use (ajuste o catálogo):

```sql
SELECT table_schema, table_name, table_type, data_source_format
FROM workspace.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'self_service')
ORDER BY table_schema, table_name;
```

As tabelas devem ter formato `DELTA`. Bronze/Silver usam nomes como `public__application`; Gold usa `application`. Os três schemas são criados pela Bronze. O [relatório](VERIFICACAO.md) distingue verificação local de execução real.

## 10. Dashboards e novas tabelas

Use cada arquivo em `dashboards/` como dataset. Ajuste `main.self_service` para o catálogo/schema configurado no Job. Os parâmetros, gráficos e granularidades estão no [README principal](../README.md).

Para adicionar uma nova Gold, copie `_template.py.txt`, implemente seu SQL e declare `SILVER_TABLES` e `GOLD_DEPENDENCIES`. Adicione uma task ao job após `silver` e os contextos necessários e inclua-a nas dependências de `validar`. Se mantiver as barreiras atuais de lotes, faça a nova task aguardar o lote anterior; a quantidade de tasks pode ser revista conforme o compute e as dependências reais. Nenhum notebook Gold existente precisa ser alterado. Veja o [guia Gold](../notebooks/gold/README.md).
