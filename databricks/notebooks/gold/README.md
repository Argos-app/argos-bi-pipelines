# Gold modular no mesmo Job

Cada arquivo `.py` grava uma única tabela Delta de mesmo nome em `self_service`.
São 30 tabelas finais e cinco contextos auxiliares, criados na primeira execução.
O recurso do Job continua sendo `argos_medallion`, com nome base `argos-bronze-silver-self-service` (o target `dev` pode acrescentar um prefixo). O grafo atual contém 38 tasks: Bronze, Silver, 35 Gold e validação.

```text
bronze → silver → lote 1 → lote 2 → lote 3 → lote 4 → lote 5 → lote 6 → lote 7 → validar
```

São sete lotes de até cinco tasks. Cada lote aguarda todas as tasks do anterior com `run_if: ALL_SUCCESS`; cada task também mantém `silver` e os contextos Gold que lê como dependências. O lote 1 contém os cinco contextos, com suas dependências internas preservadas. Os lotes seguintes contêm as tabelas finais. Uma falha bloqueia os lotes posteriores.

Cada notebook contém seu SQL de negócio e cria uma única tabela Delta. O schema `self_service` é criado pela Bronze; o runner também usa `CREATE SCHEMA IF NOT EXISTS`. Nenhuma tabela de destino precisa existir antes da primeira execução.

## Adicionar uma nova tabela

1. Copie `_template.py.txt` para `<nome>.py` como notebook e descreva seu objetivo.
2. Defina `GOLD_TABLE`, seu SQL e `SILVER_TABLES`. Declare os contextos Gold usados
   em `GOLD_DEPENDENCIES`, se houver. O runner valida somente essas entradas e
   grava apenas o destino indicado.
3. Adicione uma task Notebook ao **mesmo Job**, apontando para esse arquivo. Em
   `depends_on`, use `silver`, as tasks dos contextos utilizados e **todas as tasks do lote anterior**. Cada lote aceita até cinco tasks; o próximo notebook inicia o lote 8, pois os sete lotes atuais estão completos. Passe os mesmos
   parâmetros de catálogo/schema. Use `run_if: ALL_SUCCESS`.
4. Acrescente a task às dependências de `validar` no próprio Job para que a
   validação final aguarde todas as gravações. Não altere outros notebooks Gold.

A atualização limita-se ao novo notebook, à configuração de tasks/dependências e à documentação. Não é necessário registrar o novo SQL em `argos/gold.py`, criar pipelines, ou modificar consultas/notebooks das tabelas já existentes.
Se uma nova Silver for dividida em atividades específicas no futuro, ajuste as
dependências da nova task para os produtores reais; a Silver atual não foi alterada.

## Tabelas, objetivos e dependências

| Tabela / notebook | Objetivo | Silver lida diretamente | Contextos Gold lidos |
|---|---|---|---|
| `_context_user_property` | Relacionar proprietários e usuários vinculados às fazendas sem repetir vínculos. | `property`, `user_property` | — |
| `_context_medicine` | Identificar contextos de uso de medicamentos em aplicações e movimentações. | `application`, `herd`, `lot`, `medicine_batch`, `stock_movement` | — |
| `_context_medicine_batch` | Identificar contextos de uso por lote de medicamento. | `application`, `herd`, `lot`, `stock_movement` | — |
| `_context_supplier` | Relacionar fornecedores aos contextos de uso de seus medicamentos. | `medicine` | `_context_medicine` |
| `_context_therapeutic_category` | Relacionar categorias terapêuticas aos contextos de uso. | `medicine` | `_context_medicine` |
| `access_audit` | Disponibilizar auditoria de acessos com contexto atual do usuário. | `access_audit`, `app_user`, `property` | `_context_user_property` |
| `alert` | Disponibilizar alertas por destinatário, animal/lote e fazenda conhecida. | `alert`, `animal_exception`, `app_user`, `herd`, `lot`, `property` | — |
| `animal_exception` | Disponibilizar animais em exceção, situação sanitária e fazenda atual. | `animal_exception`, `app_user`, `herd`, `lot`, `property` | — |
| `animal_lot_history` | Disponibilizar o histórico de permanência dos animais nos lotes. | `animal_lot_history`, `app_user`, `herd`, `lot`, `property` | — |
| `app_user` | Disponibilizar usuários e seus vínculos atuais com fazendas. | `app_user`, `property` | `_context_user_property` |
| `application` | Disponibilizar aplicações com responsável, fazenda e medicamento para análise sanitária. | `app_user`, `application`, `herd`, `lot`, `medicine`, `medicine_batch`, `property`, `supplier`, `therapeutic_category` | — |
| `application_animal` | Disponibilizar aplicações individuais, doses e pesos observados por animal. | `animal_exception`, `app_user`, `application`, `application_animal`, `herd`, `lot`, `medicine`, `medicine_batch`, `property`, `supplier`, `therapeutic_category` | — |
| `attachment` | Disponibilizar anexos e seus autores, sem inferir fazenda de entidade polimórfica. | `app_user`, `attachment`, `property` | — |
| `change_audit` | Disponibilizar alterações auditadas e contexto atual do autor conhecido. | `app_user`, `change_audit`, `property` | `_context_user_property` |
| `daily_access` | Disponibilizar acessos diários e contexto atual de usuários/fazendas. | `app_user`, `daily_access`, `property` | `_context_user_property` |
| `data_catalog` | Disponibilizar o dicionário e as regras de negócio dos dados. | `data_catalog` | — |
| `device` | Disponibilizar dispositivos dos usuários e seus vínculos atuais com fazendas. | `app_user`, `device`, `property` | `_context_user_property` |
| `dim_date` | Disponibilizar o calendário para análises temporais. | `dim_date` | — |
| `herd` | Disponibilizar rebanhos, raças e quantidade atual de cabeças por fazenda. | `app_user`, `herd`, `property` | — |
| `interaction_audit` | Disponibilizar interações de uso com contexto atual do usuário. | `app_user`, `interaction_audit`, `property` | `_context_user_property` |
| `load_error` | Disponibilizar rejeições e erros das rotinas de carga. | `load_error` | — |
| `load_mapping` | Disponibilizar os mapeamentos entre identificadores de origem e destino. | `load_mapping` | — |
| `load_run` | Disponibilizar a execução e a situação das rotinas de carga. | `load_run` | — |
| `load_staging` | Disponibilizar os registros e a situação do estágio de carga. | `load_staging` | — |
| `lot` | Disponibilizar lotes de animais, categorias, rebanhos e fazendas. | `app_user`, `herd`, `lot`, `property` | — |
| `medicine` | Disponibilizar o catálogo de medicamentos, fornecedor, categoria e contextos de uso. | `app_user`, `medicine`, `property`, `supplier`, `therapeutic_category` | `_context_medicine` |
| `medicine_batch` | Disponibilizar lotes de medicamentos, validade e contextos de uso. | `app_user`, `medicine`, `medicine_batch`, `property`, `supplier`, `therapeutic_category` | `_context_medicine_batch` |
| `notification_delivery` | Disponibilizar entregas de notificações e o contexto do alerta. | `alert`, `animal_exception`, `app_user`, `herd`, `lot`, `notification_delivery`, `property` | — |
| `occurrence` | Disponibilizar ocorrências sanitárias e sua resolução por animal e fazenda. | `animal_exception`, `app_user`, `herd`, `lot`, `occurrence`, `property` | — |
| `permission` | Disponibilizar perfis de permissão com contexto de usuários e fazendas. | `app_user`, `permission`, `property` | `_context_user_property` |
| `property` | Disponibilizar o cadastro das fazendas e seus proprietários. | `app_user`, `property` | — |
| `stock_movement` | Disponibilizar entradas e saídas por medicamento, autor e fazenda conhecida. | `app_user`, `application`, `herd`, `lot`, `medicine`, `medicine_batch`, `property`, `stock_movement`, `supplier`, `therapeutic_category` | — |
| `supplier` | Disponibilizar fornecedores relacionados aos contextos dos medicamentos fornecidos. | `app_user`, `property`, `supplier` | `_context_supplier` |
| `therapeutic_category` | Disponibilizar categorias terapêuticas e seus contextos de uso. | `app_user`, `property`, `therapeutic_category` | `_context_therapeutic_category` |
| `user_property` | Disponibilizar as relações entre usuários e fazendas. | `app_user`, `property`, `user_property` | — |

Os contextos podem ter dependências transitivas em outras tabelas Silver. Todas
essas leituras são precedidas pelo sucesso de `silver` e dos respectivos contextos.
Os dados e nomes usados pelos dashboards permanecem os mesmos.
