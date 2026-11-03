SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT request_route,request_method,count(*) AS requests_terminais,
       count(request_status_code) AS com_status_http,
       count_if(request_status_code BETWEEN 200 AND 399) AS bons,
       count_if(request_status_code BETWEEN 400 AND 499) AS cliente_4xx,
       count_if(request_status_code>=500 OR status IN ('failed','timeout')) AS falhas_servidor_ou_transporte,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(request_status_code),0) AS fracao_http_bom,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(*),0) AS fracao_conservadora_observada
FROM __safe_g_observations
WHERE execution_mode='real' AND operation_kind='http'
  AND operation_name='http.request' AND status<>'started'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY request_route,request_method
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
