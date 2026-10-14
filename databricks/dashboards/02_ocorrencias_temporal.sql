-- Barras: X=mes; Y=ocorrencias; série=type.
SELECT date_trunc('MONTH', occurrence_date) AS mes,
       sf_property_id, sf_property_name, type,
       count(*) AS ocorrencias,
       sum(CASE WHEN resolved THEN 1 ELSE 0 END) AS resolvidas,
       avg(CASE WHEN resolved THEN datediff(resolved_at, occurrence_date) END) AS dias_medios_resolucao
FROM main.self_service.occurrence
WHERE true
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
  AND (:data_inicio IS NULL OR occurrence_date >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR occurrence_date < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY date_trunc('MONTH', occurrence_date), sf_property_id, sf_property_name, type
ORDER BY mes, sf_property_name, type;
