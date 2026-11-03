SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT service,operation_kind,operation_name,observation_level,agent,
       coalesce(model_returned,model_requested) AS modelo, request_route, status,
       count(*) AS amostras,
       percentile_approx(duration_ms,0.50,10000) AS p50_ms,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms,
       percentile_approx(duration_ms,0.99,10000) AS p99_ms
FROM __safe_g_observations
WHERE execution_mode='real' AND status<>'started' AND duration_ms IS NOT NULL
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY service,operation_kind,operation_name,observation_level,agent,
         coalesce(model_returned,model_requested),request_route,status
) dataset
CROSS JOIN (
SELECT CASE WHEN EXISTS (
 SELECT 1 FROM workspace.gold_ia._publication p
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
 ON p.env=r.env AND p.published_batch_id=r.run_id
 WHERE p.env='argos'
) THEN 1 ELSE raise_error('BI de IA sem lote validado; consulte a execucao do Job.') END AS ok
) disponibilidade WHERE disponibilidade.ok=1;
