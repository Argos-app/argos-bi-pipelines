# Configuração do BI de IA com MongoDB

O bundle padrão `databricks.yml` configura o Job `iniciar → bronze → silver_gold → validar`. A Bronze acessa o Mongo com PyMongo e lê snapshots completos. A Silver e a Gold não se conectam ao Mongo. A execução da pipeline não substitui a conferência dos dados e dashboards no workspace.

## Conexão

1. Garanta acesso de rede do compute do Job ao Mongo, resolução DNS/SRV, TLS e autorização do banco. O usuário precisa de `find` nas collections e autorização para `listCollections` nos databases usados, pois a Bronze verifica se cada collection existe antes de tratar uma resposta vazia como snapshot vazio.
2. Abra `notebooks/01_bronze.py` no Workspace e preencha a conexão diretamente no notebook, como na Bronze relacional:

   ```python
   mongo_uri = 'mongodb+srv://USUARIO:SENHA@HOST/?authSource=admin&tls=true'
   ```

   Use a URI fornecida para seu Mongo. Codifique caracteres especiais do usuário e da senha conforme as regras de URI. O valor não é parâmetro do Job. A Bronze recusa o marcador `PREENCHA_URI_MONGODB`. Restrinja o acesso ao notebook preenchido e não versione credenciais; um novo deploy pode substituir a cópia do notebook no Workspace.
3. Configure `source_database` (padrão `argos_ai`) e `source_a2a_database` (vazio reutiliza o principal). Ambas as origens usam a mesma URI Mongo.
4. Atribua a `mongo_source_id` um identificador estável e não secreto do cluster e das origens. Ao trocar de cluster, altere esse identificador na configuração da Bronze e da pipeline. A assinatura vincula as duas às mesmas origens sem guardar a URI ou credenciais.

O conector lê todas as 15 origens lógicas habilitadas. As quatro origens de workflow usam `ai_email_workflows`, separadas por `record_type`; valor desconhecido bloqueia a carga. Uma collection ausente ou uma falha de leitura interrompe o Job, sem confirmar o lote global. Collections existentes e vazias são válidas. Consultas leem as collections sequencialmente: não existe snapshot transacional entre collections, nem garantia de que uma collection permaneça imutável durante a leitura. Evite cargas concorrentes e valide a reconciliação com a origem.

## Bundle e publicação

Crie os schemas de destino em um catálogo Unity Catalog apropriado ou autorize a Bronze a criá-los. Os schemas padrão são `bronze_ia`, `silver_ia` e `gold_ia`. Confira objetos existentes, pois tabelas Delta anteriores e materialized views não são intercambiáveis. Ajuste o catálogo e os schemas no bundle e nos SQLs de dashboard; configure permissões e dependências. A task Bronze instala PyMongo; a pipeline declara sua dependência no ambiente.

Execute `databricks bundle validate`, faça o deploy conforme a configuração do workspace e rode apenas o Job, sem execução paralela da Bronze ou da pipeline isolada. A task `iniciar` invalida a publicação e a task `validar` confirma o lote somente depois da pipeline e das validações. Confira a execução e os resultados antes de usar os dashboards. Consulte [Lakeflow](../lakeflow/README.md) para as limitações.

A mudança para Mongo não migra nem remove dados históricos.
