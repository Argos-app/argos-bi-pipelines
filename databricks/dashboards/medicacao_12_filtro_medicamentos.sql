SELECT DISTINCT id AS medicamento_id
FROM workspace.self_service.medicine
WHERE id IS NOT NULL
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
ORDER BY medicamento_id;
