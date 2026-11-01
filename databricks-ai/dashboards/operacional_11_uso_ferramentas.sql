SELECT target_name,count(*) AS invocacoes,
       count_if(terminal_status IS NULL) AS sem_terminal,
       count_if(terminal_status='succeeded')*1.0/nullif(count(terminal_status),0) AS sucesso_tecnico,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.50,10000) AS p50_ms,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.95,10000) AS p95_ms,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.99,10000) AS p99_ms
FROM workspace.gold_ia.g_invocations
WHERE target='tool'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
GROUP BY target_name;

