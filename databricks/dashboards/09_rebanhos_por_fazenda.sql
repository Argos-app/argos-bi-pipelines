-- Barras: X=sf_property_name; Y=cabecas. Estado atual do rebanho.
-- Não representa evolução histórica de head_count.
SELECT sf_property_id, sf_property_name, breed,
       count(*) AS rebanhos, sum(head_count) AS cabecas
FROM main.self_service.herd
WHERE active = true
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
GROUP BY sf_property_id, sf_property_name, breed
ORDER BY sf_property_name, breed;
