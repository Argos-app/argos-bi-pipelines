WITH dados AS (
  SELECT
    dose,
    unit,
    sf_medicine_id,
    sf_medicine_name,
    floor(try_divide(dose, :largura_bin_dose)) * :largura_bin_dose AS faixa_inicio
  FROM
    main.self_service.application
  WHERE
    active = true
    AND :largura_bin_dose > 0
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
      OR application_date >= CAST(:data_inicio AS DATE)
    )
    AND (
      :data_fim IS NULL
      OR application_date < date_add(CAST(:data_fim AS DATE), 1)
    )
)
SELECT
  sf_medicine_id,
  sf_medicine_name,
  unit,
  faixa_inicio,
  faixa_inicio + :largura_bin_dose AS faixa_fim,
  count(*) AS frequencia
FROM
  dados
GROUP BY
  sf_medicine_id,
  sf_medicine_name,
  unit,
  faixa_inicio
ORDER BY
  sf_medicine_name,
  unit,
  faixa_inicio;