-- Dropdown :medicamentos; catálogos enriquecidos por contexto de uso.
SELECT
  DISTINCT id AS medicamento_id,
  brand_name AS medicamento_nome
FROM
  main.self_service.medicine
WHERE
  (
    :usuarios IS NULL
    OR array_contains(:usuarios, sf_user_id)
  )
  AND (
    :fazendas IS NULL
    OR array_contains(:fazendas, sf_property_id)
  )
ORDER BY
  medicamento_nome;