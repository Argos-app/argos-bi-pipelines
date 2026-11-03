WITH dados AS (
  SELECT
    sf_property_id,
    sf_property_name,
    floor(try_divide(weight_at_time, :largura_bin_kg)) * :largura_bin_kg AS faixa_inicio_kg
  FROM
    workspace.self_service.application_animal
  WHERE
    sf_application_active = true
    AND weight_at_time IS NOT NULL
    AND weight_at_time > 0
    AND :largura_bin_kg > 0
    AND (
      :usuarios IS NULL
      OR array_contains(:usuarios, sf_user_id)
    )
    AND (
      :fazendas IS NULL
      OR array_contains(:fazendas, sf_property_id)
    )
    AND (
      :medicamentos IS NULL
      OR array_contains(:medicamentos, sf_medicine_id)
    )
    AND (
      :data_inicio IS NULL
      OR sf_application_date >= CAST(:data_inicio AS DATE)
    )
    AND (
      :data_fim IS NULL
      OR sf_application_date < date_add(CAST(:data_fim AS DATE), 1)
    )
)
SELECT
  sf_property_id,
  sf_property_name,
  faixa_inicio_kg,
  faixa_inicio_kg + :largura_bin_kg AS faixa_fim_kg,
  count(*) AS frequencia
FROM
  dados
GROUP BY
  sf_property_id,
  sf_property_name,
  faixa_inicio_kg
ORDER BY
  sf_property_name,
  faixa_inicio_kg;