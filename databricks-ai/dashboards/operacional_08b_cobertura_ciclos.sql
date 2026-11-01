SELECT link_status,count(*) AS perguntas_iniciais,
       count_if(root_interaction_id IS NULL) AS sem_raiz_reconstruida
FROM workspace.gold_ia.g_interactions
WHERE interaction_kind='message'
  AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
GROUP BY link_status;