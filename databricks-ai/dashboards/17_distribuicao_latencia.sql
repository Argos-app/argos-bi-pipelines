-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q17. Amostras individuais para histograma e boxplot nativos de latência.
SELECT started_at, service, operation_kind, operation_name, agent, request_route,
       status, duration_ms, user_id, farm_ids
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND status <> 'started' AND duration_ms IS NOT NULL
  AND started_at >= CAST(:inicio AS TIMESTAMP) AND started_at < CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint);
