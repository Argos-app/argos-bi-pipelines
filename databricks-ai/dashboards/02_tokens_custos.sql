-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold definida em exemplos-camadas.sql e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim); ambiente obrigatório.
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q02. Tokens e custo CONHECIDOS, separados por moeda/origem. Só chamadas canônicas.
WITH b AS (
  SELECT * FROM ai_bi.gold.g_observations
  WHERE env=:ambiente AND execution_mode='real' AND status<>'started'
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (replayed_result IS NULL OR replayed_result=false)
    AND ((operation_kind='model' AND operation_name='model.invoke' AND observation_level='sdk_invocation')
      OR (operation_kind='model' AND operation_name='model.a2a.answer.generate' AND observation_level='transport_attempt')
      OR (operation_kind='embedding' AND operation_name='embedding.a2a.embed' AND observation_level='transport_attempt'))
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
    AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
    AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
)
SELECT agent, provider, coalesce(model_returned,model_requested) AS modelo,
       operation_kind, billing.currency AS moeda, billing.cost_source AS origem_custo,
       count(*) AS chamadas, sum(usage.input_tokens) AS entrada_conhecida,
       sum(usage.output_tokens) AS saida_conhecida, sum(usage.total_tokens) AS total_reportado,
       sum(usage.cache_read_tokens) AS cache_leitura_conhecido,
       sum(usage.cache_write_tokens) AS cache_escrita_conhecido,
       sum(usage.reasoning_tokens) AS raciocinio_conhecido,
       sum(billing.reported_cost) AS custo_conhecido,
       count(usage.input_tokens)*1.0/nullif(count(*),0) AS cobertura_entrada,
       count(usage.output_tokens)*1.0/nullif(count(*),0) AS cobertura_saida,
       count(billing.reported_cost)*1.0/nullif(count(*),0) AS cobertura_custo,
       count_if(attempt_visibility='internal_retries_unknown') AS retries_internos_desconhecidos
FROM b
GROUP BY agent,provider,coalesce(model_returned,model_requested),operation_kind,
         billing.currency,billing.cost_source;

