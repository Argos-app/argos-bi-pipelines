WITH entregas AS (
  SELECT DISTINCT id, alert_id, device_id, channel, status, attempts, error
  FROM workspace.self_service.notification_delivery
  WHERE (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
    AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
)
SELECT channel AS canal, status, count(*) AS entregas,
       count(DISTINCT alert_id) AS alertas, sum(attempts) AS tentativas_registradas,
       count_if(error IS NOT NULL AND trim(error) <> '') AS com_erro_informado
FROM entregas
GROUP BY channel, status
ORDER BY canal, status;
