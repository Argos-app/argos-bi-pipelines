SELECT date_trunc('MONTH', occurrence_date) AS mes, type AS tipo,
       count(*) AS ocorrencias, count_if(resolved = true) AS resolvidas,
       count_if(resolved = false) AS abertas,
       avg(CASE WHEN resolved = true AND resolved_at >= occurrence_date
                THEN datediff(resolved_at, occurrence_date) END) AS media_dias_resolucao
FROM main.self_service.occurrence
WHERE 1=1
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
  AND (:data_inicio IS NULL OR occurrence_date >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR occurrence_date < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY date_trunc('MONTH', occurrence_date), type
ORDER BY mes, tipo;
