SELECT request_route,request_method,count(*) AS requests_terminais,
       count(request_status_code) AS com_status_http,
       count_if(request_status_code BETWEEN 200 AND 399) AS bons,
       count_if(request_status_code BETWEEN 400 AND 499) AS cliente_4xx,
       count_if(request_status_code>=500 OR status IN ('failed','timeout')) AS falhas_servidor_ou_transporte,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(request_status_code),0) AS fracao_http_bom,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(*),0) AS fracao_conservadora_observada
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND operation_kind='http'
  AND operation_name='http.request' AND status<>'started'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY request_route,request_method;

