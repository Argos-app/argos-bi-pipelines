# Verificação da instalação nova

Revisão em 14/10/2026. Destinos inicialmente sem schemas ou tabelas. A Bronze cria os schemas e metadados; as três camadas criam suas próprias tabelas Delta. A Gold preserva os 35 notebooks e sete lotes de até cinco tasks no mesmo Job.

**42 testes locais: 0 falhas, 0 erros, 0 ignorados.**

Os testes locais verificam SQL e resultados em DuckDB, UPPER/deduplicação e snapshots, conversão TIME/TIMETZ no SELECT PostgreSQL, criação de schema/tabela Silver e Gold quando os destinos não existem, proteção de destinos incompatíveis, dependências de negócio, barreiras de lotes, 30 cenários de durações assíncronas, falhas, dashboards e links da documentação.

DuckDB 1.5.6, SQLGlot 30.22.0 e PyYAML 6.0.3 são usados nos testes. As consultas são transpiladas; os testes de escrita usam adaptador em memória e dados sintéticos. Isso não é execução nativa Spark/Delta nem conexão com o banco real.

| Verificação remota | Estado |
|---|---|
| JDBC, senha, rede Aiven e driver PostgreSQL | Não executado |
| Primeira ingestão Bronze real e criação dos três schemas no workspace | Não executado |
| Execução das 38 tasks em Spark/Delta | Não executado |
| Catálogo/permissões e presença das 30 tabelas na origem | Não executado |
| Configuração e renderização dos dashboards | Não executado |

Para reproduzir localmente, na pasta `databricks`:

```powershell
python -m pip install --target .test-deps -r tests/requirements.txt
python -m unittest discover -s tests -p "test_*.py" -v
```

Depois siga o [passo a passo do zero](CONFIGURACAO_DATABRICKS.md) e execute **Run now**. Confirme sucesso nas 38 tasks, incluindo `validar`, e as tabelas Delta nos três schemas. O [README](../README.md) descreve a semântica das camadas.
