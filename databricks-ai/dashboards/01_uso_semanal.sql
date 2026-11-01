-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q01. Uso semanal e WAU (semanas parciais visíveis pelas datas dos parâmetros).
WITH b AS (
  SELECT *, date_trunc('week',from_utc_timestamp(created_at,'America/Sao_Paulo')) AS semana
  FROM workspace.gold_ia.g_interactions
  WHERE created_at>=CAST(:inicio AS TIMESTAMP)
    AND created_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL
      OR coalesce(array_contains(farm_ids,:fazenda),false))
)
SELECT semana, count(*) AS interacoes, count(DISTINCT user_id) AS wau,
       count(DISTINCT session_id) AS sessoes,
       count_if(interaction_kind='message') AS perguntas_iniciais,
       count_if(interaction_kind='answer') AS continuacoes,
       count_if(interaction_status='completed') AS concluidas,
       min(created_at) AS primeira_atividade, max(created_at) AS ultima_atividade,
       CAST(:inicio AS TIMESTAMP) AS janela_inicio, CAST(:fim AS TIMESTAMP) AS janela_fim
FROM b GROUP BY semana ORDER BY semana;

