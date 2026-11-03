SELECT DISTINCT sf_property_id AS fazenda_id
FROM workspace.self_service.app_user
WHERE sf_property_id IS NOT NULL
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
ORDER BY fazenda_id;
