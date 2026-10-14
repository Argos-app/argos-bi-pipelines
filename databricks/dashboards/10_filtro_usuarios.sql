-- Dataset exclusivo do dropdown multisseleção :usuarios.
SELECT DISTINCT sf_user_id AS usuario_id, sf_user_name AS usuario_nome
FROM main.self_service.app_user
WHERE sf_user_id IS NOT NULL
ORDER BY usuario_nome;
