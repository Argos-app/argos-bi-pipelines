SELECT state,count(*) AS operacoes,
       count(*)*1.0/nullif(sum(count(*)) OVER(),0) AS proporcao,
       percentile_approx(CASE WHEN state='pending'
          THEN timestampdiff(SECOND,created_at,current_timestamp()) END,0.95,10000) AS pending_idade_p95_s,
       count_if(error_code IS NOT NULL) AS com_codigo_erro
FROM workspace.gold_ia.g_email_dispatches
WHERE created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
GROUP BY state;

