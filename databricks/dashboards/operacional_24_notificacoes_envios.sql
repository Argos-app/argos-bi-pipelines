WITH entregas AS (
  SELECT DISTINCT id, alert_id, channel, status, sent_at, attempts
  FROM workspace.self_service.notification_delivery
  WHERE sent_at IS NOT NULL
    AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
    AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
    AND (:data_inicio IS NULL OR sent_at >= CAST(:data_inicio AS DATE))
    AND (:data_fim IS NULL OR sent_at < date_add(CAST(:data_fim AS DATE), 1))
)
SELECT CAST(sent_at AS DATE) AS dia_envio, channel AS canal, status,
       count(*) AS registros_com_data_envio, count(DISTINCT alert_id) AS alertas,
       sum(attempts) AS tentativas_registradas
FROM entregas
GROUP BY CAST(sent_at AS DATE), channel, status
ORDER BY dia_envio, canal, status;
