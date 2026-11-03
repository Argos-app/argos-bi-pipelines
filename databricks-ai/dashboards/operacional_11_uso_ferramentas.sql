SELECT dataset.* FROM (
WITH __safe_g_invocations AS (
 SELECT g.* FROM workspace.gold_ia.g_invocations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT target_name,count(*) AS invocacoes,
       count_if(terminal_status IS NULL) AS sem_terminal,
       count_if(terminal_status='succeeded')*1.0/nullif(count(terminal_status),0) AS sucesso_tecnico,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.50,10000) AS p50_ms,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.95,10000) AS p95_ms,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.99,10000) AS p99_ms
FROM __safe_g_invocations
WHERE target='tool'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
GROUP BY target_name
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
