SELECT CAST(r.start_time AS DATE) AS dia_inicio, r.routine_name AS rotina, r.source AS origem,
       e.source_table AS tabela_origem, e.reprocessed AS reprocessado,
       count(*) AS registros_com_erro, sum(e.attempts) AS tentativas_registradas
FROM workspace.self_service.load_error e
JOIN workspace.self_service.load_run r ON r.id = e.load_run_id
WHERE (:data_inicio IS NULL OR r.start_time >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR r.start_time < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY CAST(r.start_time AS DATE), r.routine_name, r.source, e.source_table, e.reprocessed
ORDER BY dia_inicio, rotina, tabela_origem, reprocessado;
