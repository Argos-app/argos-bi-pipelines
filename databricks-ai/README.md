# Argos BI — IA

Pipelines de IA separadas em `databricks-ai/`, com a mesma organização das pipelines relacionais em `databricks/`.

```text
databricks-ai/
├── argos/                    módulos das camadas e delta_layers.py
├── dashboards/               um SQL por dataset, sem persistir indicadores
├── docs/                     configuração, arquitetura e verificação
│   └── entregaveis/          versões autocontidas, ipynb e ZIPs
├── notebooks/                Bronze, Silver, validação e fixture
│   └── gold/                 uma task por tabela Gold
├── tests/                    testes, requirements e ferramentas de geração
├── databricks.yml            Job com sete tasks
└── README.md
```

- [Começar do zero e configurar a conexão MongoDB](docs/CONFIGURACAO_DATABRICKS.md)
- [Arquitetura e fontes examinadas](docs/plano-databricks-ia.md)
- [Verificação local e pendências no workspace](docs/VERIFICACAO.md)
- [Objetivos e dependências das tabelas Gold](notebooks/gold/README.md)
- [Notebooks autocontidos para copiar/importar](docs/entregaveis/copiar_colar/)
- [Pacotes ZIP](docs/entregaveis/pacotes/)

Fluxo: `01_bronze → 02_silver → gold/<cada_tabela> → 04_validacao`.
As camadas usam tabelas Delta comuns; Gold somente seleciona e une Silver. Métricas ficam nos SELECTs de dashboard.

Cada projeto tem sua própria pasta `argos/`: importe a raiz correta no Workspace e não misture os módulos de IA com os relacionais.
As pipelines relacionais existentes foram preservadas.
