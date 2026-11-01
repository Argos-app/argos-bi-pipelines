SELECT service,operation_kind,operation_name,observation_level,agent,
       coalesce(model_returned,model_requested) AS modelo, request_route, status,
       count(*) AS amostras,
       percentile_approx(duration_ms,0.50,10000) AS p50_ms,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms,
       percentile_approx(duration_ms,0.99,10000) AS p99_ms
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND status<>'started' AND duration_ms IS NOT NULL
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY service,operation_kind,operation_name,observation_level,agent,
         coalesce(model_returned,model_requested),request_route,status;

