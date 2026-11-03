WITH lotes AS (
  SELECT DISTINCT id, medicine_id, sf_medicine_name, expiry_date, status
  FROM workspace.self_service.medicine_batch
  WHERE 1=1
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
    AND (:medicamentos IS NULL OR array_contains(:medicamentos, medicine_id))
), classificados AS (
  SELECT *, CASE WHEN expiry_date IS NULL THEN 'SEM DATA'
    WHEN expiry_date < current_date() THEN 'VENCIDO'
    WHEN expiry_date <= date_add(current_date(), 30) THEN 'ATE 30 DIAS'
    ELSE 'ACIMA DE 30 DIAS' END AS faixa_validade
  FROM lotes
)
SELECT medicine_id, sf_medicine_name, faixa_validade, status, count(*) AS lotes
FROM classificados
GROUP BY medicine_id, sf_medicine_name, faixa_validade, status
ORDER BY sf_medicine_name, faixa_validade;
