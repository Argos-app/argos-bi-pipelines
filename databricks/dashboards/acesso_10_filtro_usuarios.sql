SELECT DISTINCT sf_user_id AS usuario_id
FROM workspace.self_service.app_user
WHERE sf_user_id IS NOT NULL
ORDER BY usuario_id;
