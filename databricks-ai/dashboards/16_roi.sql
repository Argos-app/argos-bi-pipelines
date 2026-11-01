-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q16. ROI informado para a mesma coorte e moeda: entradas são premissas auditáveis.
-- O retorno e o custo total devem cobrir o mesmo período/filtros. Não inferir receita.
WITH p AS (
 SELECT try_cast(:retorno_usd AS DOUBLE) AS retorno,
        try_cast(:custo_total_usd AS DOUBLE) AS custo
)
SELECT retorno AS retorno_bruto_usd, custo AS custo_total_usd,
       CASE WHEN retorno >= 0 AND custo > 0 THEN (retorno-custo)/custo END AS roi,
       CASE WHEN retorno >= 0 AND custo > 0 THEN retorno-custo END AS retorno_liquido_usd
FROM p;

