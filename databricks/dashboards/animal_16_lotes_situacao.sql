SELECT sf_property_id, sf_property_name, category AS categoria, is_exception_lot,
       fit_for_slaughter, count(*) AS lotes, sum(head_count) AS cabecas
FROM main.self_service.lot
WHERE active = true
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
GROUP BY sf_property_id, sf_property_name, category, is_exception_lot, fit_for_slaughter
ORDER BY sf_property_name, categoria;
