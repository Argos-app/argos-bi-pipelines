-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q04. União de intervalos de agentes FILHOS DIRETOS de cada grafo.
-- Não usa SUM(duration_ms) como tempo de resposta. Relógios apenas do mesmo serviço.
-- Sem filtro de agente para não transformar decomposição em execução parcial escondida.
WITH g AS (
  SELECT * FROM workspace.gold_ia.g_observations
  WHERE execution_mode='real' AND operation_kind='graph'
    AND operation_name IN ('graph.execute','graph.resume') AND status<>'started'
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
), children AS (
  SELECT g.observation_id AS graph_id, g.env, a.observation_id AS agent_span,
         a.started_at AS s,a.ended_at AS e,a.status
  FROM g JOIN workspace.gold_ia.g_observations a
    ON a.env=g.env AND a.parent_observation_id=g.observation_id
   AND a.service=g.service AND a.operation_kind='agent' AND a.execution_mode='real'
), valid AS (
  SELECT c.*,max(e) OVER(PARTITION BY env,graph_id ORDER BY s,e,agent_span
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
  FROM children c JOIN g ON c.env=g.env AND c.graph_id=g.observation_id
  WHERE c.status<>'started' AND c.e>=c.s AND c.s>=g.started_at AND c.e<=g.ended_at
), marked AS (
  SELECT *,CASE WHEN prev_end IS NULL OR s>prev_end THEN 1 ELSE 0 END AS new_island
  FROM valid
), numbered AS (
  SELECT *,sum(new_island) OVER(PARTITION BY env,graph_id ORDER BY s,e,agent_span
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island
  FROM marked
), islands AS (
  SELECT env,graph_id,island,min(s) AS s,max(e) AS e
  FROM numbered GROUP BY env,graph_id,island
), spans AS (
  SELECT env,graph_id,
         sum(timestampdiff(MILLISECOND,s,e)) AS busy_ms,
         timestampdiff(MILLISECOND,min(s),max(e)) AS envelope_ms,
         count(*) AS ilhas
  FROM islands GROUP BY env,graph_id
), coverage AS (
  SELECT c.env,c.graph_id,count(*) AS filhos,
         count_if(c.status='started' OR c.e IS NULL) AS incompletos,
         count_if(v.agent_span IS NOT NULL) AS validos
  FROM children c LEFT JOIN valid v
    ON c.env=v.env AND c.graph_id=v.graph_id AND c.agent_span=v.agent_span
  GROUP BY c.env,c.graph_id
)
SELECT g.operation_name,count(*) AS grafos,
       count_if(c.filhos IS NULL OR c.validos<>c.filhos) AS sem_decomposicao_completa,
       percentile_approx(g.duration_ms,0.50,10000) AS graph_p50_ms,
       percentile_approx(g.duration_ms,0.95,10000) AS graph_p95_ms,
       percentile_approx(g.duration_ms,0.99,10000) AS graph_p99_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.busy_ms END,0.95,10000) AS agentes_ocupados_p95_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.envelope_ms-s.busy_ms END,0.50,10000) AS gaps_p50_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.envelope_ms-s.busy_ms END,0.95,10000) AS gaps_p95_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.envelope_ms-s.busy_ms END,0.99,10000) AS gaps_p99_ms
FROM g LEFT JOIN spans s ON g.env=s.env AND g.observation_id=s.graph_id
LEFT JOIN coverage c ON g.env=c.env AND g.observation_id=c.graph_id
GROUP BY g.operation_name;

