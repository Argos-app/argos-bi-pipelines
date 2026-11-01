-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q07. Custo por interação da COORTE criada no período, todo ciclo da interação disponível.
-- Não limitar spans por started_at: incluir términos tardios da mesma interação.
-- USD apenas; moedas outras ficam desconhecidas para esta medida. Sem conversão implícita.
WITH cohort AS (
  SELECT * FROM workspace.gold_ia.g_interactions
  WHERE created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
), cost AS (
  SELECT env,interaction_id,count(*) AS n,
         count_if(billing.reported_cost IS NOT NULL AND billing.currency='USD' AND status<>'started') AS priced,
         sum(CASE WHEN billing.currency='USD' AND status<>'started' THEN billing.reported_cost END) AS usd,
         count_if(billing.cost_source='configured_rate') AS estimated,
         count_if(attempt_visibility='internal_retries_unknown') AS opaque
  FROM workspace.gold_ia.g_observations
  WHERE execution_mode='real' AND replayed_result IS NOT TRUE
    AND ((operation_kind='model' AND operation_name='model.invoke' AND observation_level='sdk_invocation')
      OR (operation_kind='model' AND operation_name='model.a2a.answer.generate' AND observation_level='transport_attempt')
      OR (operation_kind='embedding' AND operation_name='embedding.a2a.embed' AND observation_level='transport_attempt'))
  GROUP BY env,interaction_id
), j AS (
  SELECT i.*,c.n,c.priced,c.usd,c.estimated,c.opaque
  FROM cohort i LEFT JOIN cost c ON i.env=c.env AND i.interaction_id=c.interaction_id
)
SELECT interaction_status,count(*) AS interacoes,
       count_if(n IS NULL) AS sem_chamadas_observadas,
       sum(n) AS chamadas_observadas,sum(priced) AS chamadas_precificadas,
       sum(usd) AS custo_modelos_usd_conhecido,
       sum(usd)/nullif(count(*),0) AS custo_parcial_usd_por_interacao,
       sum(usd)/nullif(count(*),0)
        +try_cast(:custo_ia_nao_modelo_coorte_usd AS DOUBLE)
          /nullif((SELECT count(*) FROM cohort),0) AS custo_parcial_sistema_usd_por_interacao_com_rateio,
       count_if(n=priced AND n>0 AND interaction_status IN ('completed','degraded','failed','interrupted')) AS interacoes_cobertas_observadas,
       sum(CASE WHEN n=priced AND n>0 AND interaction_status IN ('completed','degraded','failed','interrupted') THEN usd END)
        /nullif(count_if(n=priced AND n>0 AND interaction_status IN ('completed','degraded','failed','interrupted')),0)
         AS custo_medio_usd_interacao_coberta,
       sum(estimated) AS chamadas_tarifa_configurada,sum(opaque) AS chamadas_retry_opaco
FROM j GROUP BY interaction_status;

