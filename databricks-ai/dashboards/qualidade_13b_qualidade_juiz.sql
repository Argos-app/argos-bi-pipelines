SELECT dataset.* FROM (
WITH __safe_g_invocations AS (
 SELECT g.* FROM workspace.gold_ia.g_invocations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT count(*) AS invocacoes_juiz,
       count_if(judge_parse_status='valid') AS saidas_parseaveis,
       count_if(judge_parse_status='valid')*1.0/nullif(count(*),0) AS cobertura_avaliacao,
       count_if(judge_decision='approved')*1.0/nullif(count_if(judge_parse_status='valid'),0) AS aprovacao,
       count_if(judge_decision='revise') AS revisoes,count_if(judge_decision='rejected') AS rejeicoes,
       count_if(judge_parse_status='valid' AND size(unsupported_claims)>0) AS com_alegacoes_sem_fonte
FROM __safe_g_invocations
WHERE target='agent' AND target_name='judge'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
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
