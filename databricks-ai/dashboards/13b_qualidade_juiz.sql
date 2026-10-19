-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold definida em exemplos-camadas.sql e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim); ambiente obrigatório.
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q13b. Juiz: somente saídas de eventos completas e parseadas na Silver.
SELECT count(*) AS invocacoes_juiz,
       count_if(judge_parse_status='valid') AS saidas_parseaveis,
       count_if(judge_parse_status='valid')*1.0/nullif(count(*),0) AS cobertura_avaliacao,
       count_if(judge_decision='approved')*1.0/nullif(count_if(judge_parse_status='valid'),0) AS aprovacao,
       count_if(judge_decision='revise') AS revisoes,count_if(judge_decision='rejected') AS rejeicoes,
       count_if(judge_parse_status='valid' AND size(unsupported_claims)>0) AS com_alegacoes_sem_fonte
FROM ai_bi.gold.g_invocations
WHERE env=:ambiente AND target='agent' AND target_name='judge'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false));

