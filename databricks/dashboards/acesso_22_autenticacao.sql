WITH tentativas AS (
  SELECT DISTINCT id, user_id, happened_at, source, success, failure_reason
  FROM workspace.self_service.access_audit
  WHERE (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
    AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
    AND (:data_inicio IS NULL OR happened_at >= CAST(:data_inicio AS DATE))
    AND (:data_fim IS NULL OR happened_at < date_add(CAST(:data_fim AS DATE), 1))
)
SELECT CAST(happened_at AS DATE) AS dia, source AS origem, success AS sucesso,
       failure_reason AS motivo_falha, count(*) AS tentativas,
       count(DISTINCT user_id) AS usuarios_identificados
FROM tentativas
GROUP BY CAST(happened_at AS DATE), source, success, failure_reason
ORDER BY dia, origem, sucesso, motivo_falha;
