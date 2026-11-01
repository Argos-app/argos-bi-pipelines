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

