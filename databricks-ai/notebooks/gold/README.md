# Gold de IA — objetivos e dependências

Uma task por tabela, seguindo o padrão do relacional. Todas dependem da Silver; `04_validacao` depende das quatro tasks.

| Notebook/tabela | Grão | Entidades Silver |
|---|---|---|
| g_interactions | interação | interactions, sessions, cycle_membership |
| g_observations | observação | observations, interactions, sessions, http_requests |
| g_invocations | invocação | invocations, interactions, sessions |
| g_email_dispatches | operação de envio | email_dispatches, email_report_metadata |

Somente projeções e junções. Sem agregações, indicadores ou explode para filtrar. PK e cardinalidade são verificadas antes da publicação.
