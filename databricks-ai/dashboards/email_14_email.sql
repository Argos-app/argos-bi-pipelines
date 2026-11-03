SELECT dataset.* FROM (
WITH __safe_g_email_dispatches AS (
 SELECT g.* FROM workspace.gold_ia.g_email_dispatches g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT state,count(*) AS operacoes,
       count(*)*1.0/nullif(sum(count(*)) OVER(),0) AS proporcao,
       percentile_approx(CASE WHEN state='pending'
          THEN timestampdiff(SECOND,created_at,current_timestamp()) END,0.95,10000) AS pending_idade_p95_s,
       count_if(error_code IS NOT NULL) AS com_codigo_erro
FROM __safe_g_email_dispatches
WHERE created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
GROUP BY state
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
