-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q08. Custo por resolução TÉCNICA de ciclo (não satisfação ou acurácia).
-- raíz na janela; membros e custos lidos sem corte de data para capturar esclarecimentos.
-- s_cycle_membership só marca valid após validar cadeia, pergunta, titular e ausência de branches.
WITH roots AS (
  SELECT * FROM workspace.gold_ia.g_interactions
  WHERE interaction_kind='message'
    AND interaction_id=root_interaction_id AND link_status='valid'
    AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
), members AS (
  SELECT m.*,r.created_at AS root_created
  FROM roots r JOIN workspace.gold_ia.g_interactions m
    ON r.env=m.env AND r.interaction_id=m.root_interaction_id
), children AS (
  SELECT env,source_interaction_id,count(*) AS n
  FROM workspace.gold_ia.g_interactions WHERE source_interaction_id IS NOT NULL
  GROUP BY env,source_interaction_id
), obs AS (
  SELECT env,interaction_id,count(*) AS n,
     count_if(status<>'started' AND billing.currency='USD' AND billing.reported_cost IS NOT NULL) AS priced,
     sum(CASE WHEN status<>'started' AND billing.currency='USD' THEN billing.reported_cost END) AS usd
  FROM workspace.gold_ia.g_observations
  WHERE execution_mode='real' AND replayed_result IS NOT TRUE
    AND ((operation_kind='model' AND operation_name='model.invoke' AND observation_level='sdk_invocation')
      OR (operation_kind='model' AND operation_name='model.a2a.answer.generate' AND observation_level='transport_attempt')
      OR (operation_kind='embedding' AND operation_name='embedding.a2a.embed' AND observation_level='transport_attempt'))
  GROUP BY env,interaction_id
), cycle AS (
  SELECT m.env,m.root_interaction_id,count(*) AS membros,
    count_if(m.link_status<>'valid' OR m.link_status IS NULL OR m.interaction_status<>'completed') AS invalidos,
    count_if(c.n IS NULL) AS folhas,
    count_if(c.n IS NULL AND m.interaction_status='completed' AND m.response_kind='answer') AS folhas_resposta,
    max(CASE WHEN c.n IS NULL THEN timestampdiff(MILLISECOND,m.root_created,m.updated_at) END) AS elapsed_ms,
    sum(o.usd) AS usd,sum(o.n) AS chamadas,sum(o.priced) AS precificadas,
    count_if(o.n IS NULL OR o.n<>o.priced) AS membros_sem_cobertura
  FROM members m
  LEFT JOIN children c ON m.env=c.env AND m.interaction_id=c.source_interaction_id
  LEFT JOIN obs o ON m.env=o.env AND m.interaction_id=o.interaction_id
  GROUP BY m.env,m.root_interaction_id
)
SELECT count(*) AS ciclos_validamente_vinculados,
       count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1) AS resolucoes_tecnicas,
       sum(usd) AS custo_usd_conhecido_todos_ciclos,
       sum(usd)/nullif(count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1),0)
         AS custo_parcial_usd_total_por_resolucao_tecnica,
       (sum(usd)+try_cast(:custo_ia_nao_modelo_coorte_usd AS DOUBLE))
        /nullif(count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1),0)
         AS custo_parcial_sistema_usd_por_resolucao_com_rateio,
       sum(CASE WHEN invalidos=0 AND folhas=1 AND folhas_resposta=1 THEN usd END)
        /nullif(count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1),0)
         AS custo_parcial_usd_dos_ciclos_concluidos_por_resolucao,
       count_if(membros_sem_cobertura=0) AS ciclos_com_cobertura_observada,
       sum(chamadas) AS chamadas,sum(precificadas) AS precificadas,
       percentile_approx(CASE WHEN invalidos=0 AND folhas=1 AND folhas_resposta=1 THEN elapsed_ms END,0.95,10000)
         AS p95_ciclo_incluindo_espera_humana_ms
FROM cycle;

