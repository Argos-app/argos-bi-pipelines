WITH acessos AS (
  SELECT DISTINCT id, user_id, date, source, session_count
  FROM workspace.self_service.daily_access
  WHERE 1=1
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
  AND (:data_inicio IS NULL OR date >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR date < date_add(CAST(:data_fim AS DATE), 1))
 )
SELECT date AS dia, source AS origem, count(DISTINCT user_id) AS usuarios,
       sum(session_count) AS sessoes
FROM acessos
GROUP BY date, source
ORDER BY dia, origem;
