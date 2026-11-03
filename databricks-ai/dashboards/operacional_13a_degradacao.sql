SELECT dataset.* FROM (
WITH __safe_g_interactions AS (
 SELECT g.* FROM workspace.gold_ia.g_interactions g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT response_kind,count(*) AS interacoes_terminais,
       count_if(interaction_status='degraded') AS degradadas,
       count_if(interaction_status='degraded')*1.0/nullif(count(*),0) AS taxa_degradacao_populacao_filtrada,
       count_if(result_parse_status='invalid') AS resultado_invalido
FROM __safe_g_interactions
WHERE interaction_status IN ('completed','degraded','failed','interrupted')
  AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:motivo),'') IS NULL OR coalesce(array_contains(degradation_reasons,:motivo),false))
GROUP BY response_kind
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
