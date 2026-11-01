-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q06. Throughput por hora de conclusão e sucesso técnico por agente.
SELECT date_trunc('hour',ended_at) AS hora_utc,service,agent,operation_name,
       count(*) AS concluidas_na_hora,
       count(*)*3600.0/nullif(timestampdiff(SECOND,
         greatest(date_trunc('hour',ended_at),CAST(:inicio AS TIMESTAMP)),
         least(date_trunc('hour',ended_at)+INTERVAL 1 HOUR,CAST(:fim AS TIMESTAMP))),0)
         AS chamadas_por_hora_equivalente,
       count_if(status='succeeded')*1.0/nullif(count(*),0) AS sucesso_tecnico,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND operation_kind='agent' AND status<>'started'
  AND ended_at>=CAST(:inicio AS TIMESTAMP) AND ended_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
GROUP BY date_trunc('hour',ended_at),service,agent,operation_name,
  greatest(date_trunc('hour',ended_at),CAST(:inicio AS TIMESTAMP)),
  least(date_trunc('hour',ended_at)+INTERVAL 1 HOUR,CAST(:fim AS TIMESTAMP));

