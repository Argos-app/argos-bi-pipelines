SELECT started_at, service, operation_kind, operation_name, agent, request_route,
       status, duration_ms, user_id, farm_ids
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND status <> 'started' AND duration_ms IS NOT NULL
  AND started_at >= CAST(:inicio AS TIMESTAMP) AND started_at < CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint);
