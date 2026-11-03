WITH eventos AS (
  SELECT DISTINCT id, user_id, created_at, action_type, element_type, element_id, element_name
  FROM workspace.self_service.interaction_audit
  WHERE (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
    AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
    AND (:data_inicio IS NULL OR created_at >= CAST(:data_inicio AS DATE))
    AND (:data_fim IS NULL OR created_at < date_add(CAST(:data_fim AS DATE), 1))
)
SELECT CAST(created_at AS DATE) AS dia, action_type AS acao, element_type AS tipo_elemento,
       element_id, element_name AS elemento, count(*) AS interacoes,
       count(DISTINCT user_id) AS usuarios
FROM eventos
GROUP BY CAST(created_at AS DATE), action_type, element_type, element_id, element_name
ORDER BY dia, acao, tipo_elemento, elemento;
