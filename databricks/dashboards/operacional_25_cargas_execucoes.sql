SELECT CAST(start_time AS DATE) AS dia_inicio, routine_name AS rotina, source AS origem,
       status, count(*) AS execucoes,
       count_if(end_time IS NOT NULL AND end_time >= start_time) AS com_duracao,
       avg(CASE WHEN end_time IS NOT NULL AND end_time >= start_time
                THEN timestampdiff(SECOND, start_time, end_time) END) AS duracao_media_segundos,
       sum(rows_read) AS linhas_lidas, sum(rows_inserted) AS linhas_inseridas,
       sum(rows_updated) AS linhas_atualizadas, sum(rows_rejected) AS linhas_rejeitadas,
       sum(rows_rejected) * 1.0 / nullif(sum(rows_read), 0) AS proporcao_rejeitada
FROM workspace.self_service.load_run
WHERE (:data_inicio IS NULL OR start_time >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR start_time < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY CAST(start_time AS DATE), routine_name, source, status
ORDER BY dia_inicio, rotina, origem, status;
