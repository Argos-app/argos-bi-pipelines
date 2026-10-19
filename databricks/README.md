# Argos — três camadas em tabelas Delta comuns

Bronze, Silver e Gold/Self Service agora são **tabelas Delta comuns**, gerenciadas pelo mesmo Job, com uma atividade por tabela Gold:

```text
01_bronze → 02_silver → gold/<cada_tabela>.py → 04_validacao
```

A Bronze mantém append JDBC; a Silver e a Gold atualizam snapshots com `format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(...)`. Não há decorators Lakeflow, criação de materialized views ou task de ETL pipeline no Job atual. Cada write Delta é atômico por tabela; o conjunto de todas as tabelas não é uma transação única. Durante uma execução, diferentes tabelas podem estar em pontos diferentes da atualização.

## Arquivos principais

| Arquivo | Uso |
|---|---|
| `notebooks/01_bronze.py` | Ingestão PostgreSQL em append |
| `notebooks/02_silver.py` | Estado atual, UPPER e deduplicação; snapshot em Delta |
| `notebooks/gold/*.py` | Um notebook por tabela Gold/contexto; joins e snapshot em Delta |
| `notebooks/04_validacao.py` | Verifica Delta físico, PK e cardinalidade |
| `argos/delta_layers.py` | Criação e gravação batch de tabelas Delta |
| `argos/common.py`, `bronze.py`, `silver.py`, `gold.py` | Funções e transformações compartilhadas |
| `databricks.yml` | Bundle do mesmo Job com Bronze, Silver, atividades Gold independentes e validação |
| `dashboards/*.sql` | 12 datasets SQL de gráficos e filtros |


## Instalação nova pela interface

Comece pelo [passo a passo do zero](docs/CONFIGURACAO_DATABRICKS.md). Escolha um catálogo existente, importe `argos/` e `notebooks/`, preencha a conexão Bronze e crie **Jobs & Pipelines → Create → Job**. O projeto não exige schemas ou tabelas de destino previamente criados.

A Bronze cria automaticamente `bronze`, `silver` e `self_service`, além dos metadados e tabelas de ingestão. Silver e Gold criam seus destinos na sequência. A primeira carga é completa porque não há watermark. Nas cargas seguintes, o mesmo Job continua usando append na Bronze e snapshots Delta na Silver/Gold.

O Job tem 38 tasks e a Gold usa sete lotes de até cinco atividades. A matriz de notebooks e dependências está no guia. O [relatório de verificação](docs/VERIFICACAO.md) registra os testes locais e as verificações pendentes no workspace.

## Conexão direta no notebook Bronze

Preencha no `01_bronze.py`:

```python
jdbc_user = 'avnadmin'
jdbc_password = 'PREENCHA_A_SENHA_DO_BANCO'
```

A URL padrão já utiliza o host/porta/banco Aiven informados e `sslmode=require`. A senha é definida diretamente no notebook, conforme solicitado; não há secret scope. O código falha antes de consultar o banco se o marcador continuar preenchido. Usuário/senha não são widgets nem parâmetros do Job. Limite o acesso ao notebook preenchido e evite versionar a senha. Ao usar bundle, um deploy substitui a cópia do notebook no Workspace.

O `databricks.yml` mantém a configuração equivalente como referência. Para configurar manualmente, siga o guia; não é necessário CLI ou deploy.

## Semântica das camadas

**Bronze:** descobre todas as tabelas físicas do schema PostgreSQL, inclusive tabelas sem timestamps. Com `updated_at`, usa ingestão incremental por timestamp com lookback e anti-join de PK/timestamp/hash. Sem `updated_at`, usa snapshot completo em append para capturar alterações em `alert`, `device`, `daily_access`, `user_property` e outras tabelas mutáveis. Preserva versões na Bronze e registra snapshots vazios em `_ingestion_state`. `_table_manifest` registra as PKs, inclusive compostas.

`force_snapshot=true` força uma carga completa para conciliar exclusões físicas e alterações anteriores ao lookback. Depois volte a `false`. `updated_at` deve ser atualizado corretamente pela origem, como previsto pelos triggers do Argos. As leituras JDBC de várias tabelas não constituem uma única transação do PostgreSQL.

**Silver:** usa a mesma consulta de estado atual, com UPPER em strings e uma versão por PK ordenada por `_change_at`, `_ingested_at` e hash. O snapshot substitui a tabela inteira, incluindo o caso de resultado vazio. Depois de um snapshot de conciliação, versões anteriores à base não ressuscitam registros excluídos. Sem PK, remove duplicatas do conteúdo normalizado. Como a leitura reavalia o histórico Bronze, volumes grandes podem exigir uma evolução futura para processamento incremental; esta versão prioriza a correção e a simplicidade do Job.

**Gold/Self Service:** mantém os joins anteriores, sem métricas agregadas. As tabelas `_context_*` também são Delta comuns e são gravadas antes de seus dependentes. Fatos operacionais preservam uma linha por PK. Catálogos de medicamentos, lotes, fornecedores e categorias usam contextos DISTINCT de uso; usuários e auditorias podem ter uma linha por registro/fazenda vinculada. Não some medidas desses contextos como se fossem fatos. A área e o proprietário da fazenda estão em colunas `sf_*`; autor de evento e proprietário são campos distintos.

Medicamentos são globais no banco. Movimentos sem aplicação associada mantêm fazenda NULL, em vez de serem atribuídos a todas as fazendas do usuário. `attachment.entity_id` é polimórfico sem catálogo de tipos no DDL; preserva o autor e deixa a fazenda NULL. `data_catalog`, `load_*`, `dim_date` e tabelas sem relacionamento conhecido mantêm o registro com contexto NULL. A fazenda de ocorrências vem do lote atual do animal; vínculos de usuários representam relações atuais, não localização histórica de um acesso.


O UPPER abrange todas as strings, inclusive IDs, URLs, tokens e JSON quando representado como texto. Consulte a Bronze para valores originais com distinção de maiúsculas/minúsculas. Os filtros de dashboards servem para análise; permissões de dados devem ser configuradas no Unity Catalog.

## Dashboards

Cole cada SQL de `dashboards/` como dataset separado. As agregações ficam nesses datasets:

| Arquivo | Visualização |
|---|---|
| `01_aplicacoes_temporal.sql` | Barras: mês × aplicações/atendimentos; separar medicamento e unidade |
| `02_ocorrencias_temporal.sql` | Barras: mês × ocorrências; cor por tipo |
| `03_estoque_temporal.sql` | Barras: mês × entradas/saídas/saldo do intervalo |
| `04_histograma_doses.sql` | Frequências por faixa de dose |
| `05_histograma_pesos.sql` | Frequências por faixa de peso observado |
| `06_boxplot_pesos_observacoes.sql` | Boxplot ou histograma nativo de peso por fazenda |
| `07_boxplot_doses_observacoes.sql` | Boxplot ou histograma nativo de dose por medicamento/unidade |
| `08_boxplot_pesos_estatisticas.sql` | Quartis, IQR, bigodes de Tukey e outliers para tabela/visual customizado |
| `09_rebanhos_por_fazenda.sql` | Barras do estado atual do número de cabeças por fazenda/raça |
| `10_filtro_usuarios.sql` | Opções do filtro de usuários |
| `11_filtro_fazendas.sql` | Opções do filtro de fazendas |
| `12_filtro_medicamentos.sql` | Opções do filtro de medicamentos |

Configure `usuarios`, `fazendas` e `medicamentos` como String com **Allow multiple selections**, usando os IDs dos datasets de opções. `All` fornece NULL. Os filtros `array_contains` são aplicados antes da agregação. `data_inicio` e `data_fim` são Date; o fim é inclusivo. `largura_bin_dose` e `largura_bin_kg` são Numeric > 0, com padrões sugeridos 1 e 50. Faixas inválidas retornam um dataset vazio.

Boxplots nativos usam os datasets de observações, sem quartis pré-agregados. Um animal pode ter várias observações de peso em aplicações diferentes. `animais_aplicados` representa atendimentos, não animais únicos. Não some medicamentos/unidades diferentes; o saldo do SQL 03 é do intervalo selecionado. O número atual de cabeças não permite reconstruir uma série histórica sem snapshots históricos específicos.

## Validação e testes

`04_validacao.py` verifica o tipo de todos os objetos das três camadas, exigindo `MANAGED`/`EXTERNAL` com formato `DELTA`. Verifica PKs na Silver, conservação de registros nos joins e cardinalidade dos fatos. Objetos incompatíveis nos destinos fazem o Job falhar antes dos writes.

Os testes locais exercitam SQL transpilado para DuckDB e a criação de destinos e execução de snapshots com um adaptador Spark em memória. Eles não substituem a execução JDBC/Spark/Delta no workspace.

```powershell
python -m pip install --target .test-deps -r tests/requirements.txt
python -m unittest discover -s tests -p "test_*.py" -v
```

Referências: [writes e substituição de schema Delta](https://docs.databricks.com/aws/en/tables/update-schema), [tipos dos objetos no Unity Catalog](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/tables), [notebooks em Jobs](https://docs.databricks.com/aws/en/jobs/configure-task) e [parâmetros dos dashboards](https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters).


## Adicionar uma tabela self-service

Copie `notebooks/gold/_template.py.txt` para um novo notebook, implemente o SQL e declare `SILVER_TABLES` e `GOLD_DEPENDENCIES`. Acrescente sua task ao próximo lote disponível do mesmo Job, aguardando todas as tasks do lote anterior, e às dependências de `validar`. O lote atual só pode ter até cinco tasks. Nenhum outro notebook Gold precisa ser alterado. Consulte o [guia Gold](notebooks/gold/README.md).
