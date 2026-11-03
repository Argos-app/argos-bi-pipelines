SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT date_trunc('hour',ended_at) AS hora_utc,service,agent,operation_name,
       count(*) AS concluidas_na_hora,
       count(*)*3600.0/nullif(timestampdiff(SECOND,
         greatest(date_trunc('hour',ended_at),CAST(:inicio AS TIMESTAMP)),
         least(date_trunc('hour',ended_at)+INTERVAL 1 HOUR,CAST(:fim AS TIMESTAMP))),0)
         AS chamadas_por_hora_equivalente,
       count_if(status='succeeded')*1.0/nullif(count(*),0) AS sucesso_tecnico,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms
FROM __safe_g_observations
WHERE execution_mode='real' AND operation_kind='agent' AND status<>'started'
  AND ended_at>=CAST(:inicio AS TIMESTAMP) AND ended_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
GROUP BY date_trunc('hour',ended_at),service,agent,operation_name,
  greatest(date_trunc('hour',ended_at),CAST(:inicio AS TIMESTAMP)),
  least(date_trunc('hour',ended_at)+INTERVAL 1 HOUR,CAST(:fim AS TIMESTAMP))
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
