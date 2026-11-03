SELECT CAST(r.start_time AS DATE) AS dia_inicio, r.routine_name AS rotina, r.source AS origem,
       s.processed AS processado, count(*) AS registros_staging
FROM workspace.self_service.load_staging s
JOIN workspace.self_service.load_run r ON r.id = s.load_run_id
WHERE (:data_inicio IS NULL OR r.start_time >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR r.start_time < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY CAST(r.start_time AS DATE), r.routine_name, r.source, s.processed
ORDER BY dia_inicio, rotina, origem, processado;
