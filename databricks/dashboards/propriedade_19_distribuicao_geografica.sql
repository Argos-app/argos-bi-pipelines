SELECT state AS estado, city AS cidade, count(DISTINCT id) AS propriedades,
       sum(area_hectares) AS area_hectares
FROM main.self_service.property
WHERE active = true
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
GROUP BY state, city
ORDER BY estado, cidade;
