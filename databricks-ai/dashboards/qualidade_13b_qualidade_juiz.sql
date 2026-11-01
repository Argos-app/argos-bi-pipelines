SELECT count(*) AS invocacoes_juiz,
       count_if(judge_parse_status='valid') AS saidas_parseaveis,
       count_if(judge_parse_status='valid')*1.0/nullif(count(*),0) AS cobertura_avaliacao,
       count_if(judge_decision='approved')*1.0/nullif(count_if(judge_parse_status='valid'),0) AS aprovacao,
       count_if(judge_decision='revise') AS revisoes,count_if(judge_decision='rejected') AS rejeicoes,
       count_if(judge_parse_status='valid' AND size(unsupported_claims)>0) AS com_alegacoes_sem_fonte
FROM workspace.gold_ia.g_invocations
WHERE target='agent' AND target_name='judge'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false));

