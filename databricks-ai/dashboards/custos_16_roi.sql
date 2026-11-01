WITH p AS (
 SELECT try_cast(:retorno_usd AS DOUBLE) AS retorno,
        try_cast(:custo_total_usd AS DOUBLE) AS custo
)
SELECT retorno AS retorno_bruto_usd, custo AS custo_total_usd,
       CASE WHEN retorno >= 0 AND custo > 0 THEN (retorno-custo)/custo END AS roi,
       CASE WHEN retorno >= 0 AND custo > 0 THEN retorno-custo END AS retorno_liquido_usd
FROM p;

