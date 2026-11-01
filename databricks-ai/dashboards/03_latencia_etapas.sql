-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q03. Latência observada por etapa/nível/estado: inclui HTTP total e graph wall time.
SELECT service,operation_kind,operation_name,observation_level,agent,
       coalesce(model_returned,model_requested) AS modelo, request_route, status,
       count(*) AS amostras,
       percentile_approx(duration_ms,0.50,10000) AS p50_ms,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms,
       percentile_approx(duration_ms,0.99,10000) AS p99_ms
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND status<>'started' AND duration_ms IS NOT NULL
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY service,operation_kind,operation_name,observation_level,agent,
         coalesce(model_returned,model_requested),request_route,status;

