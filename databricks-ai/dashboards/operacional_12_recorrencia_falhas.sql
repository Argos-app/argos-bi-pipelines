WITH b AS (
  SELECT *,lag(started_at) OVER(PARTITION BY env,service,agent,operation_name,error.type,error.code
     ORDER BY started_at,observation_id) AS previous_failure
  FROM workspace.gold_ia.g_observations
  WHERE execution_mode='real' AND status IN ('failed','timeout')
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
    AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
    AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
    AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
    AND (nullif(trim(:falha),'') IS NULL OR error.type=:falha)
)
SELECT date_trunc('day',started_at) AS dia_utc,service,agent,operation_kind,operation_name,
       observation_level,error.origin AS origem,error.type AS tipo,error.code AS codigo,
       request_route,coalesce(model_returned,model_requested) AS modelo,
       count(*) AS falhas,count(DISTINCT interaction_id) AS interacoes_afetadas,
       count_if(attempt_reason='retry_transient') AS retries,
       percentile_approx(timestampdiff(SECOND,previous_failure,started_at),0.50,10000) AS mediana_seg_entre_falhas
FROM b GROUP BY date_trunc('day',started_at),service,agent,operation_kind,operation_name,
       observation_level,error.origin,error.type,error.code,request_route,coalesce(model_returned,model_requested);

