# Pipeline Lakeflow IA

Verifique a execução do Job completo, as permissões e a publicação do lote antes de utilizar os dashboards.

A configuração `databricks.yml` descreve o bundle Lakeflow. O fluxo é `iniciar → bronze → silver_gold → validar`. `iniciar` invalida `_publication`; `validar` só confirma o último lote Bronze depois de verificar qualidade, chaves e cardinalidade Gold. Os SQLs de dashboard filtram pela publicação confirmada e pelo último lote Bronze. Durante a atualização ou após falha os datasets protegidos devem sinalizar indisponibilidade; confirme esse comportamento no warehouse antes de usá-los. Isso pressupõe executar exclusivamente o Job, sem executar a pipeline isoladamente nem permitir outra Bronze simultânea.

A Bronze usa PyMongo e snapshots completos das 15 origens lógicas; uma collection existente e vazia é válida. O parser e schemas vêm da pasta `argos`, e PyMongo é dependência do ambiente da pipeline. A identidade `mongo_source_id`, os databases, as collections, o catálogo e os schemas devem coincidir entre Bronze e pipeline. Configure as URIs diretamente no notebook Bronze conforme o guia de configuração. Um catálogo ou schema diferente também exige atualizar os prefixos nos SQLs.

A reconstrução de vínculos usa CTE recursiva limitada: ciclos sem raiz ficam `unknown`; bifurcações tornam a cadeia `ambiguous`. A profundidade máxima padrão é 100. Segundo a documentação do Databricks, CTE recursiva em materialized view exige recálculo completo; limites de tamanho de resultado e versão de runtime precisam ser verificados no workspace.

Não misture os destinos da versão anterior baseada em tabelas Delta com as materialized views; confira os objetos já existentes antes de definir os schemas. Não há garantia de atomicidade entre materialized views. Restrições de acesso devem impedir execução direta da pipeline, leituras da Gold sem os SQLs protegidos e execução concorrente com outros Jobs.
