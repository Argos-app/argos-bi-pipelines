-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold definida em exemplos-camadas.sql e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim); ambiente obrigatório.
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q12. Recorrência de erros (eventos observados, não incidentes raiz deduplicados).
WITH b AS (
  SELECT *,lag(started_at) OVER(PARTITION BY env,service,agent,operation_name,error.type,error.code
     ORDER BY started_at,observation_id) AS previous_failure
  FROM ai_bi.gold.g_observations
  WHERE env=:ambiente AND execution_mode='real' AND status IN ('failed','timeout')
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

