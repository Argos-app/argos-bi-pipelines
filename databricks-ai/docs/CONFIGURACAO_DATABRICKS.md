# Argos IA — pipelines completas para Databricks

> Local no repositório: `databricks-ai/`, separado das pipelines existentes em `databricks/`.
> Notebooks prontos: `docs/entregaveis/copiar_colar/`. Código modular: `argos/` e `notebooks/`.
> Plano e exemplos: `docs/`. Arquivos ZIP: `docs/entregaveis/pacotes/`. Queries: `dashboards/`.

Implementação baseada na organização de `argos-bi-repository/databricks`: **Job batch com tabelas Delta comuns**, Bronze → Silver → uma task por Gold → validação. Não usar “Create ETL pipeline”/Lakeflow Declarative Pipelines para estes notebooks: escolher **Jobs & Pipelines → Create → Job**, como no repositório atual. Não há decorators DLT, views ou materializações de indicadores.

O código está preparado para importação/configuração. Foram executados 23 testes locais de contratos, BSON, SQL e DAG. **Não foi executado no seu workspace nem conectado ao seu MongoDB**. O notebook opcional `05_testes_fixture` executa as transformações em Spark/Delta com dados sintéticos em schemas isolados antes da primeira carga real.

## 1. Forma mais simples: importar os notebooks autocontidos

Use o pacote `argos-ai-bi-copiar-colar.zip`. Ele contém os notebooks completos em `.ipynb`, com células preservadas, sem depender de módulos externos. Extraia o ZIP localmente e importe os arquivos `.ipynb` no Workspace com esta organização:

```text
/Workspace/Shared/argos-ai-bi/
  notebooks/
    01_bronze
    02_silver
    gold/
      g_interactions
      g_observations
      g_invocations
      g_email_dispatches
    04_validacao
    05_testes_fixture         # opcional; não entra no Job de produção
```

Os arquivos equivalentes `.py` estão em `docs/entregaveis/copiar_colar/` no pacote completo. Podem ser importados como notebook de origem Databricks ou copiados **célula por célula**, seguindo `# COMMAND ----------`. Ao colar texto em um notebook vazio, execute `%pip install pymongo==4.19.0` em uma primeira célula real: as linhas `# MAGIC %pip ...` só viram magic automaticamente na importação de um notebook de origem. Importar `.ipynb` evita essa diferença.

Alternativa para desenvolvimento com arquivos de Workspace/Git folder: preserve a estrutura completa `argos/`, `notebooks/`, `dashboards/`, `databricks.yml`. Os notebooks modulares em `notebooks/` procuram a pasta `argos` nos diretórios ancestrais, igual ao padrão do projeto atual. Use **uma** alternativa de notebooks; não importe `.py` e `.ipynb` com o mesmo nome na mesma pasta.

## 2. Compute e catálogo

* Use um **compute classic dedicado (Dedicated access mode), Databricks Runtime 16.4 LTS ou superior**, com acesso de rede ao MongoDB e ao repositório de pacotes. Esse alvo permite os checkpoints locais dos DataFrames usados na Silver. Estes notebooks não foram adaptados para compute serverless/Spark Connect.
* Instalação `pymongo==4.19.0` ocorre na primeira célula da Bronze e Silver; PySpark e Delta vêm do Databricks Runtime. Não instalar o pacote `bson` separadamente: ele acompanha PyMongo.
* Escolha catálogo existente com permissão para criar schemas/tabelas. O padrão é `ai_bi`. Se autorizado a criar um catálogo novo, execute **uma vez** no SQL Editor:

```sql
CREATE CATALOG IF NOT EXISTS ai_bi;
```

Se sua conta não permite criar catálogo, informe um catálogo existente no parâmetro `catalog`; o código cria os schemas/tabelas. Não é necessário criar as tabelas previamente. Se alterar catálogo ou `gold_schema`, substitua também `ai_bi.gold` no arquivo de queries.

Permissões de destino: USE CATALOG/USE SCHEMA, CREATE SCHEMA onde permitido, CREATE TABLE, SELECT e MODIFY nos destinos, além de uso do compute e leitura dos secrets escolhidos. Use namespace exclusivo para cada ambiente: por exemplo `ai_bi_dev` e `ai_bi_prod`, ou conjuntos diferentes de schemas. O código bloqueia sobrescrita quando `_runs` já contém outro `environment` no mesmo namespace.

## 3. Conexão com MongoDB — duas opções prontas

O banco fonte implementado no serviço é MongoDB, padrão **`argos_ai`**; não usar JDBC PostgreSQL para extrair observações/sessões. PostgreSQL de negócio e Databricks billing não são dependências destas camadas de telemetria. Não é possível preencher host, usuário e senha reais sem os dados do ambiente; o pacote não lê nem inclui `.env` ou credenciais do repositório.

### Opção A — URI diretamente na Bronze

Abra `01_bronze` e preencha a célula final:

```python
MONGO_URI = 'mongodb+srv://USUARIO:SENHA_URL_ENCODED@HOST/?authSource=admin&tls=true'
MONGO_A2A_URI = ''  # reutiliza a conexão principal
config=read_config(dbutils)
ingest(spark,dbutils,config,MONGO_URI,MONGO_A2A_URI)
```

URI alternativa para endpoint Mongo sem SRV:

```text
mongodb://USUARIO:SENHA_URL_ENCODED@HOST:27017/?authSource=admin&tls=true
```

Use a URI fornecida pelo administrador para replica sets, certificados privados ou opções adicionais. Encode caracteres especiais da senha conforme URI (por exemplo `@` vira `%40`); não faça encode da URI inteira. `mongo_database` determina o database efetivamente lido, mesmo se a URI incluir outro database. Ajuste `authSource` à base de autenticação real.

Os dois parâmetros de secrets principais devem ficar vazios nesta opção. Não publicar uma cópia com URI preenchida. A URI não é impressa pelo código e não é parâmetro do Job. Acesso de edição ao notebook continua dando acesso à credencial embutida.

### Opção B — Secret Scope

Armazene a **URI completa** em um secret do seu workspace (por exemplo scope `argos-bi`, chave `mongo-uri`). Nos parâmetros do Job:

```text
mongo_secret_scope = argos-bi
mongo_secret_key   = mongo-uri
mongo_database     = argos_ai
```

Não precisa alterar `MONGO_URI`: o secret tem precedência. A Silver e as Gold não abrem conexão MongoDB. Para A2A em outro cluster, use `mongo_a2a_secret_scope`/`mongo_a2a_secret_key` ou preencha `MONGO_A2A_URI` na Bronze; para outro database, use `mongo_a2a_database`. Vazio reutiliza o database/conexão principal. E-mail e demais collections usam a conexão principal, conforme database compartilhado documentado.

### Rede e permissões na origem

O usuário Mongo do BI precisa somente de **find nas collections habilitadas**. O código não escreve no Mongo, não cria índices e não exige `listCollections`, `listDatabases` ou comando administrativo de ping. A primeira leitura é o teste de autenticação/autorização.

O hostname deve resolver a partir do compute Databricks; libere a rota privada ou saída/NAT/IPs efetivos do compute na origem e mantenha TLS. `localhost`, `mongodb` e nomes internos do Compose não apontam para seu computador a partir do Databricks. Para Atlas, valide também a resolução SRV/TXT. Não desabilite validação de certificado para contornar um erro de conexão; forneça a cadeia/configuração correta do endpoint.

## 4. Parâmetros e nomes das collections

Todos os notebooks usam os mesmos widgets. Os parâmetros do Job são enviados às notebook tasks pelo mecanismo nativo de Job parameters. Ao executar individualmente, preencha os mesmos valores nos widgets de cada notebook.

| Parâmetro | Padrão / finalidade |
|---|---|
| `catalog` | `ai_bi`; catálogo existente |
| `bronze_schema`, `silver_schema`, `gold_schema` | `bronze`, `silver`, `gold` |
| `environment` | `local`; identidade lógica do ambiente/cluster fonte. Não reutilizar namespace/checkpoint para outro Mongo |
| `mongo_database` | `argos_ai` |
| `mongo_secret_scope`, `mongo_secret_key` | Vazios; URI principal por secret quando preenchidos |
| `mongo_a2a_database` | Vazio reutiliza principal |
| `mongo_a2a_secret_scope`, `mongo_a2a_secret_key` | Vazios reutilizam conexão principal |
| `collections_json` | `{}`; overrides por alias abaixo |
| `force_snapshot` | `false`; primeira carga e alteração de collections já forçam snapshot. Usar `true` em conciliação periódica |
| `lookback_minutes` | `5`; releitura do incremental por updated_at |
| `page_size` | `500`; 1..1000 documentos/página |
| `max_documents_per_source` | `1000000`; ultrapassar interrompe o run sem confirmar checkpoint global |
| `max_page_bytes` | `16777216`; páginas são divididas antes da escrita; documento individual maior exige aumento explícito |
| `max_cycle_depth` | `100`; limite de reconstrução de esclarecimentos; cadeias não resolvidas ficam unknown |
| `fail_on_quarantine` | `true`; dados inválidos/conflitos bloqueiam publicação Gold. `false` exclui inválidos e mantém trilha em q_records |

| Alias | Collection padrão | Modo recorrente |
|---|---|---|
| sessions | ai_sessions | updated_at incremental |
| interactions | ai_interactions | updated_at incremental |
| messages | ai_messages | snapshot |
| execution_events | ai_execution_events | snapshot |
| observations | ai_observations | updated_at incremental |
| a2a_observations | ai_a2a_observations | updated_at incremental |
| session_summaries | ai_session_summaries | updated_at incremental |
| user_memory | ai_user_memory | updated_at incremental |
| report_artifacts | ai_report_artifacts | snapshot |
| email_preparations | ai_email_preparations | snapshot |
| pending_recipient_resolutions | ai_pending_recipient_resolutions | snapshot |
| email_authorizations | ai_email_send_authorizations | snapshot |
| email_operation_references | ai_email_operation_references | snapshot |
| email_reports | ai_email_prepared_reports | snapshot |
| email_dispatches | ai_email_dispatches | updated_at incremental |

Confirme os nomes no ambiente do serviço, especialmente `MAIN_API_OBSERVABILITY_COLLECTION` e `A2A_OBSERVABILITY_COLLECTION`. Exemplo de widget JSON:

```json
{
  "observations": {"collection": "MINHA_COLLECTION_OBSERVABILIDADE"},
  "a2a_observations": {"enabled": false}
}
```

`enabled=false` desativa a leitura dessa origem e produz a entidade Silver vazia, para preservar o schema das Gold. Não use isso para disfarçar falha de acesso. Ausência da collection e collection vazia são indistinguíveis usando somente find: ambas produzem snapshot vazio. Confirme os nomes antes da primeira execução. Dados sem `updated_at` BSON Date em uma origem incremental acionam snapshot automático; o parser ainda sinaliza campos inválidos.

Collections externas de documentos bovinos/medicamentos **não estão habilitadas por suposição**: seus nomes e contratos completos pertencem ao produtor de negócio e não são necessários às queries de telemetria. Checkpoints LangGraph são excluídos por não terem contrato analítico estável. As 15 origens acima cobrem os documentos técnicos/funcionais da IA que fundamentam esta entrega.

## 5. Criar o Job

As sete tasks:

```text
bronze (01_bronze)
  └─ silver (02_silver)
       ├─ g_interactions
       ├─ g_observations
       ├─ g_invocations
       └─ g_email_dispatches
            └─ validar (04_validacao; depende DAS QUATRO Gold)
```

Na interface, vincule o mesmo compute classic às tasks, adicione as dependências e configure **Maximum concurrent runs = 1**. Não execute Silver/Bronze manualmente enquanto outro run está ativo. Não executar `05_testes_fixture` como parte do Job de dados reais.

`docs/job-settings.json` contém o mesmo Job completo no formato de settings da Jobs API. Substitua `PREENCHA_CLUSTER_ID` e, se necessário, o prefixo `/Workspace/Shared/argos-ai-bi/notebooks/`. Configure os parâmetros antes de criar/executar. O arquivo não contém credenciais. O caminho dos notebooks **não leva extensão** após importação no Workspace.

Alternativa por bundle (estrutura modular completa): `databricks.yml` já contém o DAG. Informe `cluster_id` e os parâmetros do Job no target/Workspace. Exemplo de comandos, **não executados nesta entrega**:

```text
databricks bundle validate --var cluster_id=SEU_CLUSTER_ID
databricks bundle deploy --var cluster_id=SEU_CLUSTER_ID
```

Comece por execução manual. Depois agende, por exemplo, a cada 15 min conforme volume/custo. Programe conciliação periódica com `force_snapshot=true`; isso captura exclusões físicas e alterações anteriores ao lookback. Evite duas agendas concorrentes e desative execução paralela pelo Job. Volte `force_snapshot=false` após a conciliação.

## 6. O que cada camada implementa

**Bronze — 15 tabelas `b_<alias>`**. Guarda Extended JSON canônico integral, `_id`, origem, database/collection, hash, versão, datas, run_id e versão do extrator. O mesmo `_id` started e terminal produz versões distintas. O par updated_at/_id ordena o incremental; watermark superior e lookback controlam a janela. Em modos snapshot, cursor é `_id`. Memória do driver é limitada por página; limite total de documentos falha explicitamente em vez de truncar.

`_ingestion_state` confirma cada leitura; `_runs` confirma o conjunto somente quando todas as origens terminam. Silver e checkpoints futuros só usam runs com commit global. Uma queda após gravar páginas deixa linhas de run não confirmado fora do consumo. Reexecutar recebe novo run_id; a Bronze registra nova evidência de extração. A chave de MERGE inclui run_id/origem/id/hash: deduplicação de páginas dentro do run, preservação entre snapshots/runs, deduplicação do documento corrente na Silver. Não há “exactly once” entre Mongo e Delta nem snapshot transacional entre collections.

**Silver — 14 entidades base**, já que API e A2A são unificadas em `s_observations`, com identificação de origem. Todas são tabelas Delta tipadas e atualizadas por snapshot, como no BI atual, mas **sem UPPER**. Campos novos/ignorados pelo schema continuam íntegros na Bronze. Parser não usa os serviços da aplicação nem depende de suas bibliotecas; schemas foram transcritos dos contratos examinados.

Entidades: `s_sessions`, `s_interactions`, `s_messages`, `s_execution_events`, `s_observations`, `s_session_summaries`, `s_user_memory`, `s_report_artifacts`, `s_email_preparations`, `s_pending_recipient_resolutions`, `s_email_authorizations`, `s_email_operation_references`, `s_email_reports`, `s_email_dispatches`.

Derivadas estruturais: `s_event_messages` (posexplode de mensagens de execução), `s_observation_resources` (posexplode de recursos), `s_invocations` (pareamento início/término), `s_cycle_membership` (cadeia de esclarecimento), `s_http_requests` (raiz HTTP), `s_email_report_metadata` (metadados sem corpo/base64/e-mail), `s_linkage_issues` (órfãos). Não são tabelas de indicadores.

`farm_ids`, `degradation_reasons` e `unsupported_claims` permanecem arrays. Custo usa DECIMAL(38,20); recurso e grandezas de relatório DECIMAL(30,10), rejeitando precisão excedente sem arredondamento silencioso. Datas ISO dos envelopes JSON são convertidas para DATE/TIMESTAMP; BSON Date exige UTC. O parser preserva ausência/NULL/array vazio, e envia tipos inválidos/elementos nulos para quarentena. `result_json` inválido mantém a interação com result_parse_status invalid; juiz truncado não vira aprovação/rejeição inventada.

Eventos started/terminal têm IDs distintos: s_invocations usa invocation_id; para agentes, o início vem do evento started, e não do created_at do terminal. Dois terminais para uma invocação bloqueiam a publicação. Observações podem atualizar o mesmo ID; terminal não regride a started; terminais semanticamente divergentes são quarentena. A unicidade global dos observation_ids entre API/A2A é testada antes da junção.

Cadeias de esclarecimento são reconstruídas do payload_json da mensagem de resposta. Validam mesma sessão/usuário e question_id da pergunta fonte; ciclos, lacunas e limite de profundidade ficam unknown; bifurcações ficam ambiguous. Isso produz vínculo estrutural, não sucesso de negócio. Q08 continua sendo proxy de resolução técnica.

`q_records` armazena referências à Bronze e códigos de regras, sem copiar conteúdo sensível. É criada mesmo sem erros. Linhas inválidas são preservadas na Bronze para correção/reprocessamento. `s_linkage_issues` sinaliza órfãos legítimos por legado/TTL/atraso. Referências contraditórias de usuário/sessão bloqueiam Gold. `_publication.state=running` impede que uma Silver parcial seja usada manualmente; somente ao concluir todas as transformações o estado vira ready.

**Gold — quatro tabelas de detalhe**, sem SUM/COUNT/GROUP BY, percentis ou cálculo de indicadores:

| Tabela | Grão e junções |
|---|---|
| g_interactions | Interação; N:1 sessão + 1:0..1 vínculo do ciclo |
| g_observations | Observação; N:0..1 interação, sessão e HTTP por chave validada |
| g_invocations | Invocação; N:0..1 interação e sessão |
| g_email_dispatches | Operação; N:0..1 relatório por report_id/requested_by |

Cada task verifica PK e igualdade de cardinalidade com a entidade Silver. Conteúdo de conversa/memória, corpos de e-mail e anexos ficam fora da Gold. `s_email_report_metadata` remove endereços/nomes e base64 antes da junção; Gold só seleciona os arrays já minimizados. `04_validacao` verifica as quatro tabelas Delta, chaves, cardinalidade e lote publicado, e libera `gold._publication`.

## 7. Queries de dashboards

`dashboards/*.sql` (um arquivo por dataset) contém 17 SELECTs independentes. Cole cada bloco Q como dataset no dashboard e configure os named parameters documentados no plano. Não persistir esses SELECTs como views/tabelas/materializações. Os nomes padrão são compatíveis com as Gold criadas aqui, incluindo structs usage/billing/http/error e arrays.

A cópia consolidada fica em `docs/queries-dashboard.sql`; os 17 arquivos em `dashboards/` são os datasets individuais. As consultas usam `env=:ambiente`; informe o mesmo valor de `environment` do Job. Exemplos de parâmetros: `inicio=2026-10-19 00:00:00`, `fim=2026-10-26 00:00:00` em UTC; opcionais fazenda/agente/modelo/endpoint/falha/motivo podem ficar vazios. `array_contains` evita multiplicar registros para filtrar farm_ids/degradation_reasons.

Q02–Q08 e demais medidas de spans reais selecionam execution_mode real. Fixtures são mock; uma consulta vazia nessa população não é falha da ingestão. Dados funcionais não têm execution_mode: use ambientes separados para não misturar médias de custo de mocks e reais. Custos ausentes nunca viram zero; preços/projeções continuam parâmetros, não tabelas inventadas. Veja também `docs/plano-databricks-ia.md` e as referências a arquivos/commits da análise anterior.

## 8. Segurança, retenção e consistência

Credenciais pertencem somente à Bronze. Restrinja Bronze e Silver, pois JSONs completos podem conter e-mails, memória, relatórios e conteúdo pessoal mesmo após sanitização da aplicação. Permita ao BI apenas Gold autorizada. `farm_ids` é escopo histórico da sessão, não uma regra de segurança nem indicação de fazenda efetivamente consultada.

Snapshots completos conciliam exclusões na Silver. Nas origens incrementais, exclusões/TTL só são percebidos no próximo force_snapshot. A Bronze conserva a evidência extraída até aplicar sua política de retenção. A Silver/Gold representam o estado reconciliado dos documentos; após a expiração operacional, os registros removidos deixam essas camadas. Para consultas históricas além da retenção Mongo (por exemplo, 31 dias), defina e aprove primeiro uma política analítica de retenção/historização: não presumir que TTL autorize guardar dados pessoais indefinidamente. O histórico bruto existe na Bronze, mas estas Gold não republicam documentos excluídos nem fazem restauração automática de histórico expirado.

Writes Delta são atômicos **por tabela**, não pelo conjunto de tabelas. Consulte dashboards após o Job concluir e use `gold._publication` como marcador do último lote validado. Um consumidor externo que consultar as tabelas durante o Job pode encontrar tabelas de lotes diferentes. Para impedir essa exposição durante refresh, suspenda refreshes concorrentes ou use agendamento dependente da conclusão. Não existe troca transacional de quatro tabelas implementada aqui.

Uma collection vazia é válida e grava o estado de snapshot vazio. Uma falha de rede/permissão não é interpretada como vazio. Falhas da coleta best effort do serviço de IA continuam sendo lacuna da origem: essas pipelines não recuperam observações que nunca chegaram ao Mongo nem reconciliam faturas.

## 9. Validação e manutenção

Execute `05_testes_fixture` no compute alvo antes da carga real. Ele não conecta ao Mongo e cria schemas `*_fixture_<sufixo>` no catálogo escolhido; verifica as três camadas, grãos, esclarecimentos, Decimal128, invocações e array_contains. Os schemas de teste são preservados para inspeção; remova apenas os nomes exatos mostrados pelo notebook depois da validação, conforme suas permissões.

Instale as dependências de `tests/requirements.txt`. Testes locais (sem banco) usam PyMongo 4.19.0, SQLGlot 29.0.1 e PyYAML 6.0.3, além da biblioteca padrão:

```text
python -m unittest discover -s tests -v
```

Executar a partir de `databricks-ai/`, com essas dependências no ambiente. Testes cobrem fixtures das 15 origens, schemas aninhados, Decimal128, atualização started/terminal, custo ausente, JSON truncado, enum degraded, payload de esclarecimento, tipos inválidos, paginação, chaves de versão, SQL sem agregações na Gold e DAG do Job. Não equivalem à execução real de Spark/Delta no Databricks.

Depois de modificar os módulos, regenere notebooks e arquivos do Job:

```text
python tests/tools/build_notebooks.py
```

O pacote autocontido é gerado da mesma implementação, sem manter cópias editadas manualmente. Configuração de URI preenchida diretamente precisa ser preservada fora do versionamento e reaplicada após reimportar notebooks.

## Referências

Organização: `argos-bi-repository/databricks/notebooks/{01_bronze.py,02_silver.py,gold/*.py,04_validacao.py}` e `argos/delta_layers.py`, commit `af8791e88333527148a39e68ce065cc8e349654d`. Fontes/contratos: `argos-ai-service` feat/api, commit `d6c0349a62220ec24ffbeafb7bcbe01cb7d245bb`, especialmente packages/observability/models.py, persistence/mongo.py, persistence/email_preparations.py, packages/contracts/{email.py,lot_report.py}, docs/{dicionario-mongodb-bi.md,observabilidade-extracao-mongo.md,operacao-observabilidade.md}.

Documentação oficial consultada em 19/10/2026: [conexão PyMongo](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/connect/), [notebooks Databricks](https://docs.databricks.com/aws/en/notebooks/notebooks-code), [MERGE Delta](https://docs.databricks.com/aws/en/delta/merge). Os exemplos de conexão exigem substituição dos dados do ambiente; não afirmam uma conexão já homologada.
