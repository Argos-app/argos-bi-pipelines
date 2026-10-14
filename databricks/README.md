# Argos — Bronze, Silver, Gold/Self Service e datasets de dashboards

Implementação baseada nas 30 tabelas físicas definidas em `modulos/02_tabelas.sql` e `modulos/09_data_mart.sql` (incluindo `dim_date`) do repositório `argos-data-engineering`. A Bronze adapta o código JDBC com `for` e append fornecido na conversa. A descoberta em execução inclui todas as tabelas físicas do schema configurado, inclusive novas tabelas. Views PostgreSQL não entram na ingestão.

## Arquivos e execução

| Arquivo | Função |
|---|---|
| `notebooks/01_bronze.py` + `argos/bronze.py` | Job JDBC PostgreSQL → Delta append |
| `argos/silver.py` | UPPER e última versão por PK, inclusive PK composta |
| `argos/gold.py` | Joins com usuário, fazenda e medicamentos |
| `pipelines/02_silver_03_gold.py` | Pipeline Lakeflow declarativa com Silver e Self Service |
| `notebooks/04_validacao.py` | Checagem de PK e cardinalidade no Databricks |
| `databricks.yml` | Bundle do Job e da pipeline com dependências |
| `dashboards/*.sql` | 12 datasets SQL prontos para configurar os gráficos |
| `tests/test_medallion.py` | Testes locais de SQL e relacionamentos com dados sintéticos |

Fluxo do Job: **Bronze → pipeline Silver/Self Service → validação**. A pipeline calcula os joins a partir das tabelas Silver e define as dependências entre datasets. A Silver e a Gold são materialized views gerenciadas pelo Lakeflow. Seus resultados persistem no Unity Catalog; as queries da Gold não contêm agregações.

## Configurar no Databricks

1. Use um workspace com Unity Catalog e suporte a Lakeflow Spark Declarative Pipelines. O bundle usa pipeline serverless e um cluster existente para o JDBC e a validação. Esse cluster deve alcançar o PostgreSQL e permitir instalar a biblioteca Maven PostgreSQL especificada no bundle.
2. Abra `notebooks/01_bronze.py` e preencha `jdbc_password` na célula de conexão. `jdbc_user` está definido como `avnadmin` e a URL usa o host/porta/banco Aiven informados. As credenciais são lidas diretamente do notebook; não há configuração de secret scope. O usuário PostgreSQL precisa ler as tabelas e seus metadados em `information_schema`.

   ```python
   jdbc_user = 'avnadmin'
   jdbc_password = 'PREENCHA_A_SENHA_DO_BANCO'
   ```

   Substitua o marcador pela senha atual. A execução falha antes de consultar o banco se o marcador continuar no código. A senha ficará em texto no notebook: limite o acesso a esse arquivo e evite enviar a versão preenchida ao Git. Ao usar bundle, preencha a cópia local antes do deploy; um novo deploy substitui o notebook remoto. Usuário/senha não são enviados como widgets ou parâmetros do Job.
3. Configure a identidade do Job/pipeline com acesso aos schemas Bronze, Silver e Self Service. A Bronze cria os schemas, mas o catálogo indicado precisa existir. O notebook, a pipeline e a validação devem ter acesso aos objetos envolvidos.
4. Na pasta deste projeto, execute os comandos abaixo com a CLI Databricks autenticada no workspace correto. A URL padrão já usa o serviço Aiven informado; defina o ID do cluster existente.

```powershell
cd "C:\Users\matheusfazan-ieg\Programas\Argos\Repositorios\argos-bi-repository\databricks"
databricks bundle validate --var "cluster_id=CLUSTER_ID"
databricks bundle deploy --var "cluster_id=CLUSTER_ID"
databricks bundle run argos_medallion --var "cluster_id=CLUSTER_ID"
```

Variáveis opcionais: `catalog=main`, `source_schema=public`, `bronze_schema=bronze`, `silver_schema=silver`, `gold_schema=self_service` e `jdbc_url`. Para outro serviço, altere a URL no notebook ou sobrescreva `jdbc_url` no bundle. Mantenha usuário e senha fora da URL JDBC; eles ficam nas variáveis diretas do notebook.

O Job expõe os parâmetros `lookback_hours=24` e `force_snapshot=false`, que podem ser alterados em **Run now with different parameters**. Para conciliar exclusões físicas ou recuperar alterações mais antigas que o lookback, rode uma vez com `force_snapshot=true` e depois volte a `false`. Esse modo mantém o histórico Bronze em append e publica na Silver somente o snapshot mais recente. Não use `full_refresh` da pipeline como substituto: ele recalcula a Bronze existente, sem reler o PostgreSQL.

Se já houver tabelas Bronze produzidas pelo código-base anterior, execute a primeira carga com `force_snapshot=true`. O snapshot inicializa os novos metadados e estabelece o estado atual sem descartar o histórico anterior. Uma tentativa incremental com metadados ausentes falha com essa orientação.

O bundle não define um horário de agendamento; a frequência depende da necessidade de atualização. Configure o schedule do Job depois da primeira execução bem-sucedida. `max_concurrent_runs=1` evita ingestas concorrentes deste Job. Não rode simultaneamente outro notebook de ingestão contra as mesmas tabelas.

Também é possível importar o diretório inteiro no Workspace, executar o notebook Bronze, configurar a pipeline com o arquivo `pipelines/02_silver_03_gold.py` e encadear as três tasks na interface. Preserve a estrutura `argos/`, `notebooks/` e `pipelines/`, pois os notebooks importam os módulos compartilhados. Configure `argos.catalog`, `argos.source_schema`, `argos.bronze_schema`, `argos.silver_schema` e `argos.gold_schema` na pipeline. O `root_path` deve ser a pasta que contém esses diretórios. **Execute Bronze antes da primeira atualização da pipeline**, pois ela consulta `_table_manifest` durante o planejamento.

## Bronze: append com versões e snapshots

As tabelas têm o padrão `main.bronze.public__application`. Os valores de origem permanecem intactos. A ingestão acrescenta `_change_at`, `_row_hash`, `_batch_id`, `_ingested_at` e `_ingestion_mode`.

| Condição da tabela | Estratégia |
|---|---|
| Possui `updated_at` | Incremental com `GREATEST(created_at, updated_at)` quando ambos existem; caso contrário, `updated_at` |
| Tem apenas `created_at` ou não tem nenhum desses timestamps | Snapshot completo por execução, sempre em append |
| `force_snapshot=true` | Snapshot de todas as tabelas |

`created_at` sozinho não registra alterações: por isso `device`, `alert`, `application_animal`, `occurrence`, `daily_access`, `load_run` e outras tabelas mutáveis usam snapshots. Isso também inclui `permission`, `user_property`, `notification_delivery`, `load_error`, `load_mapping`, `load_staging` e `dim_date`. A escolha é determinada pelos metadados reais, não por uma lista fixa.

No incremental, o watermark é o maior `_change_at` já ingerido, com recuo de 24 horas por padrão. O anti-join usa PK + timestamp + hash de conteúdo e comparações null-safe. Assim, um conteúdo diferente com o mesmo timestamp ainda pode ser importado; registros já importados não se repetem a cada lookback. Timestamps nulos são incluídos. Sem PK, a identidade da versão usa o hash do conteúdo.

`_ingestion_state` registra cada carga concluída, inclusive snapshot vazio. `_table_manifest` publica as tabelas e PKs após a ingestão de todas elas. Se uma tabela já registrada desaparecer da descoberta, a Bronze falha para evitar que a pipeline exclua seus datasets automaticamente. Uma remoção intencional exige uma migração explícita do manifesto e dos datasets.

A origem deve atualizar `updated_at` corretamente; os triggers do Argos fazem isso nas tabelas correspondentes. Alterações com timestamp anterior à janela, exclusões físicas em tabelas incrementais e reversões de conteúdo que reutilizam o mesmo timestamp exigem snapshot de conciliação ou uma futura ingestão CDC. As leituras JDBC de tabelas diferentes não formam uma única transação consistente do banco. A configuração Spark usa UTC; os timestamps PostgreSQL sem timezone são mantidos como na origem.

## Silver: estado atual e UPPER

Cada tabela fica em `main.silver.public__<tabela>`. A normalização usa `UPPER` para as colunas Spark do tipo string. Depois mantém uma linha por PK, ordenada por `_change_at` e `_ingested_at`, com hash como desempate determinístico. Sem PK, remove registros idênticos após normalização. Metadados técnicos da Bronze não são expostos na Silver.

Nos snapshots, a Silver lê o último lote concluído, inclusive quando está vazio. Ao voltar ao incremental depois de um snapshot, usa esse snapshot como base e as versões posteriores, evitando ressuscitar linhas excluídas. Tipos numéricos, booleanos, datas e timestamps não recebem `UPPER`.

Conforme solicitado, o UPPER abrange todas as strings de origem, incluindo identificadores, URLs, tokens e JSON quando o JDBC os representa como texto. Para usar valores com distinção entre maiúsculas/minúsculas ou JSON original, consulte a Bronze. A Gold não tenta interpretar esses JSONs normalizados.

## Gold/Self Service: joins e granularidade

Cada tabela física descoberta tem um dataset em `main.self_service.<tabela>`. O prefixo `sf_` identifica as colunas acrescentadas pelos joins: `sf_user_id`, `sf_user_name`, `sf_property_id`, `sf_property_name`, cidade, estado, área e proprietário. Nos fatos, o usuário representa o responsável pelo evento; `sf_owner_user_id` representa o dono da fazenda.

| Dataset | Caminho de relacionamento e granularidade |
|---|---|
| `property`, `user_property`, `herd`, `lot`, `animal_exception` | Fazenda direta ou via lote/rebanho; uma linha por PK |
| `application` | Aplicação → lote → rebanho → fazenda; autor via `user_id`; lote de medicamento → medicamento → fornecedor/categoria; uma linha por aplicação |
| `application_animal` | Aplicação individual → aplicação → lote/fazenda; animal, data e status ativo da aplicação; medicamento completo; uma linha por aplicação/animal |
| `stock_movement` | Movimento → lote de medicamento e aplicação opcional → fazenda; autor do movimento; uma linha por movimento |
| `occurrence` | Ocorrência → animal → lote atual → fazenda; autor da ocorrência; uma linha por ocorrência |
| `animal_lot_history` | Histórico → lote do próprio registro → fazenda; uma linha por histórico |
| `alert`, `notification_delivery` | Alerta → lote explícito ou lote do animal → fazenda; usuário do alerta; uma linha por PK |
| `app_user`, `device`, `permission`, auditorias e `daily_access` | Usuário e vínculos atuais com fazendas; uma linha por registro e fazenda vinculada |
| `medicine`, `medicine_batch`, `supplier`, `therapeutic_category` | Contextos DISTINCT de uso em aplicações e movimentos; uma linha por entidade/usuário/fazenda, inclusive contexto NULL quando não há uso |
| `attachment` | Autor conhecido; fazenda NULL porque `entity_id` é polimórfico e o DDL não define as entidades válidas |
| `data_catalog`, `load_*`, `dim_date` e tabelas novas sem relacionamento conhecido | Registro original com contexto NULL; o modelo não fornece ligação com usuário/fazenda |

Os helpers `_context_*` usam apenas joins e remoção de relações repetidas (`DISTINCT`/`UNION`), sem métricas agregadas. Medicamentos e fornecedores são globais no banco: não há FK de estoque ou medicamento para fazenda. Uma entrada de estoque sem `application_id` permanece com fazenda NULL. Relacioná-la com todas as fazendas do autor duplicaria quantidades e atribuiria um contexto não comprovado.

Não some medidas dos catálogos contextualizados ou dos datasets de usuário sem considerar sua granularidade. Para os dashboards, use os fatos indicados abaixo, que preservam uma linha por PK. Contagem de entidades globais requer `COUNT(DISTINCT id)`.

A fazenda de `occurrence` vem do lote atual do animal, pois a ocorrência não registra lote histórico. Vínculos de usuário representam o contexto atual, não uma comprovação de em qual fazenda ocorreu um acesso. Os filtros do dashboard servem para análise; restrições de acesso devem ser aplicadas por permissões/row filters no Unity Catalog.

## Datasets dos dashboards

As queries usam `main.self_service`; troque esse prefixo se configurar outro catálogo/schema. Cole **cada arquivo separadamente** como dataset no AI/BI Dashboard. Agregações ficam nesses SQLs; a Gold permanece no nível dos registros.

| SQL | Gráfico e configuração |
|---|---|
| `01_aplicacoes_temporal.sql` | Barras: mês × aplicações ou animais aplicados; separar medicamento/unidade |
| `02_ocorrencias_temporal.sql` | Barras: mês × ocorrências; cor por tipo |
| `03_estoque_temporal.sql` | Barras: mês × entradas/saídas/saldo do período; separar medicamento/unidade |
| `04_histograma_doses.sql` | Barras das frequências já calculadas; X `faixa_inicio`, Y `frequencia` |
| `05_histograma_pesos.sql` | Barras das frequências já calculadas; X `faixa_inicio_kg`, Y `frequencia` |
| `06_boxplot_pesos_observacoes.sql` | Boxplot nativo: grupo `sf_property_name`, valor `peso_kg`; também serve para histograma nativo |
| `07_boxplot_doses_observacoes.sql` | Boxplot nativo: grupo medicamento e unidade, valor `dose`; também serve para histograma nativo |
| `08_boxplot_pesos_estatisticas.sql` | Quartis aproximados, mediana, IQR, bigodes observados dentro de 1,5 IQR e contagem de outliers; tabela ou visual customizado |
| `09_rebanhos_por_fazenda.sql` | Barras: fazenda × cabeças, cor por raça; estado atual |
| `10_filtro_usuarios.sql` | Opções do filtro de usuários |
| `11_filtro_fazendas.sql` | Opções de fazendas, com filtro por usuários |
| `12_filtro_medicamentos.sql` | Opções de medicamentos, com filtro por usuários/fazendas |

Configure `usuarios`, `fazendas` e `medicamentos` como parâmetros **String** com **Allow multiple selections** ativado. Os valores devem ser os IDs retornados pelos respectivos datasets de opções, já em maiúsculas. Use widgets de múltiplos valores, ligando os campos de IDs e os parâmetros. Os nomes servem como rótulos; IDs distinguem fazendas/medicamentos de mesmo nome. `All` fornece NULL e não restringe os resultados. Array vazio, se enviado por outro cliente, não seleciona registros.

Os filtros seguem a expressão `:fazendas IS NULL OR array_contains(:fazendas, sf_property_id)`, aplicada **antes** da agregação. Configure `data_inicio` e `data_fim` como **Date**, com valores padrão de interesse; NULL remove apenas aquele limite. O fim é inclusivo, inclusive para `movement_date` com horário. `largura_bin_dose` e `largura_bin_kg` são **Numeric**, maiores que zero, com padrões sugeridos 1 e 50; larguras inválidas retornam um dataset vazio.

Conecte os parâmetros somente aos datasets em que aparecem. Ocorrências e rebanhos não possuem filtro de medicamento. Os datasets 10–12 são exclusivos das opções de filtros. Para comparar registros sem fazenda conhecida, use `All`; escolher IDs de fazendas exclui os contextos NULL.

Pesos são observações registradas em `application_animal.weight_at_time`: o mesmo animal pode ser observado em aplicações diferentes. `animais_aplicados` conta atendimentos nas aplicações, não animais únicos. Não some doses ou estoque entre unidades/medicamentos diferentes. O saldo de estoque é o saldo **do intervalo selecionado**, não o saldo histórico acumulado. Evolução temporal usa datas dos eventos; `created_at`/`updated_at` de cadastros não permitem reconstruir a evolução histórica do número de cabeças.

## Verificação

Os 20 testes locais fazem parse no dialeto Databricks e executam SQL transpilado para DuckDB sobre o contrato das 30 tabelas e dados sintéticos. Cobrem cardinalidade dos joins, contexto de medicamentos, estoque sem fazenda, filtros antes de agregações, frequências, outliers e versões/snapshots da Silver. Não exercitam JDBC, o otimizador Spark ou a implantação do bundle.

```powershell
python -m pip install --target .test-deps -r tests/requirements.txt
python tests/test_medallion.py
```

A task `validar` roda no Databricks depois da pipeline, falhando se houver PKs nulas/duplicadas, perda de registros ou multiplicação das linhas dos fatos. A execução contra seu banco/workspace ainda depende da URL JDBC, credenciais, cluster e permissões. Não houve implantação nem execução remota nesta entrega.

Referências oficiais consultadas: [Python e loops em Lakeflow](https://docs.databricks.com/aws/en/ldp/developer/python-dev), [catálogos e schemas da pipeline](https://docs.databricks.com/aws/en/ldp/target-schema), [parâmetros multisseleção dos dashboards](https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters), [tipos de visualização AI/BI](https://docs.databricks.com/aws/en/dashboards/visualizations/types) e [driver PostgreSQL JDBC](https://jdbc.postgresql.org/download/).
