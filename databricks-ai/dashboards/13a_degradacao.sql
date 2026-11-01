-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q13a. Qualidade operacional: degradação, com filtro de presença no array.
-- População filtrada por motivo é explicitamente outra população; não chamar taxa global.
SELECT response_kind,count(*) AS interacoes_terminais,
       count_if(interaction_status='degraded') AS degradadas,
       count_if(interaction_status='degraded')*1.0/nullif(count(*),0) AS taxa_degradacao_populacao_filtrada,
       count_if(result_parse_status='invalid') AS resultado_invalido
FROM workspace.gold_ia.g_interactions
WHERE interaction_status IN ('completed','degraded','failed','interrupted')
  AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:motivo),'') IS NULL OR coalesce(array_contains(degradation_reasons,:motivo),false))
GROUP BY response_kind;

