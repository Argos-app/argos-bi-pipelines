WITH b AS (
  SELECT * FROM workspace.gold_ia.g_observations
  WHERE execution_mode='real' AND status<>'started'
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
    AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
    AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
    AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
)
SELECT operation_kind,operation_name,observation_level,agent,
       coalesce(model_returned,model_requested) AS modelo,request_route,
       count(*) AS tentativas_terminais,
       count_if(status IN ('failed','timeout')) AS erros_totais,
       count_if(status IN ('failed','timeout') AND
         (nullif(trim(:falha),'') IS NULL OR error.type=:falha)) AS erros_tipo_selecionado,
       count_if(status IN ('failed','timeout') AND
         (nullif(trim(:falha),'') IS NULL OR error.type=:falha))*1.0/nullif(count(*),0) AS indice_erro,
       count_if(status='timeout') AS timeouts,
       count_if(status='cancelled') AS cancelados,count_if(status='rejected') AS rejeitados,
       count_if(attempt_reason='retry_transient') AS retries_visiveis,
       count_if(attempt_reason='fallback') AS fallbacks_visiveis,
       count_if(attempt_visibility='internal_retries_unknown') AS visibilidade_incompleta
FROM b
GROUP BY operation_kind,operation_name,observation_level,agent,
         coalesce(model_returned,model_requested),request_route;

