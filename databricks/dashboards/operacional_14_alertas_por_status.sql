SELECT date_trunc('MONTH', created_at) AS mes, severity AS severidade, status,
       count(*) AS alertas, count_if(read_at IS NOT NULL) AS lidos
FROM workspace.self_service.alert
WHERE 1=1
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
  AND (:data_inicio IS NULL OR created_at >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR created_at < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY date_trunc('MONTH', created_at), severity, status
ORDER BY mes, severidade, status;
