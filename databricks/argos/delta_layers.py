"""Silver e Gold em tabelas Delta comuns, atualizadas por snapshots batch."""

from argos.common import source_name, sql_ident, table_name
from argos.gold import gold_queries
from argos.silver import silver_query


def namespace(catalog, schema):
    return f"{sql_ident(catalog)}.{sql_ident(schema)}"


def read_manifest(spark, config):
    schema = config.get("source_schema", "public")
    items = spark.table(
        table_name(config["catalog"], config["bronze_schema"], "_table_manifest")
    ).collect()
    items = [item for item in items if item.source_schema == schema]
    if not items:
        raise ValueError(
            "Manifesto vazio para o schema configurado. Execute a Bronze primeiro."
        )
    return items


def object_info(spark, catalog, schema, name):
    literal_schema = schema.replace("'", "''")
    literal_name = name.replace("'", "''")
    row = spark.sql(f"""SELECT table_type, data_source_format, table_owner
        FROM {namespace(catalog, 'information_schema')}.tables
        WHERE table_schema = '{literal_schema}' AND table_name = '{literal_name}'
    """).first()
    return row.asDict() if row is not None else None


def assert_delta_targets(spark, catalog, targets, require_exists=False):
    """Não substitui views ou tabelas não Delta silenciosamente."""
    errors = []
    for schema, name in targets:
        target = table_name(catalog, schema, name)
        info = object_info(spark, catalog, schema, name)
        if info is None:
            if require_exists:
                errors.append(
                    f"{target}: não existe ou não está visível para a identidade do Job"
                )
            continue
        if (
            info["table_type"] not in {"MANAGED", "EXTERNAL"}
            or str(info["data_source_format"]).upper() != "DELTA"
        ):
            errors.append(
                f"{target}: {info['table_type']} / {info['data_source_format']}"
            )
    if errors:
        raise ValueError(
            "Os destinos precisam ser tabelas Delta comuns. " + "\n".join(errors)
        )


def write_snapshot(frame, target):
    # Overwrite de uma tabela Delta: commit atômico por tabela, sem DROP TABLE.
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)


def target_inventory(spark, config, include_bronze=False):
    manifest = read_manifest(spark, config)
    catalog = config["catalog"]
    available = {item.table_name for item in manifest}
    gold = gold_queries(
        catalog,
        config["silver_schema"],
        config.get("source_schema", "public"),
        available,
    )
    targets = [
        (config["silver_schema"], source_name(item.source_schema, item.table_name))
        for item in manifest
    ]
    targets.extend((config["gold_schema"], name) for name in gold)
    if include_bronze:
        targets.extend(
            (config["bronze_schema"], source_name(item.source_schema, item.table_name))
            for item in manifest
        )
        targets.extend(
            (config["bronze_schema"], name)
            for name in ("_table_manifest", "_ingestion_state")
        )
    return targets


def run_silver(spark, config):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    catalog, schema = config["catalog"], config["silver_schema"]
    manifest = read_manifest(spark, config)
    state = table_name(catalog, config["bronze_schema"], "_ingestion_state")
    completed = {
        (row.source_schema, row.table_name)
        for row in spark.sql(
            f"SELECT DISTINCT source_schema, table_name FROM {state}"
        ).collect()
    }
    missing = {(item.source_schema, item.table_name) for item in manifest} - completed
    if missing:
        raise ValueError(
            f"Tabelas sem marcador de ingestão concluída: {sorted(missing)}. Execute Bronze."
        )
    targets = [
        (schema, source_name(item.source_schema, item.table_name)) for item in manifest
    ]
    assert_delta_targets(spark, catalog, targets)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace(catalog, schema)}")
    # Planeja todas as consultas antes de iniciar os writes.
    plans = []
    for item in manifest:
        bronze = table_name(
            catalog,
            config["bronze_schema"],
            source_name(item.source_schema, item.table_name),
        )
        target = table_name(
            catalog, schema, source_name(item.source_schema, item.table_name)
        )
        fields = [
            (field.name, field.dataType.typeName() == "string")
            for field in spark.table(bronze).schema.fields
        ]
        statement = silver_query(
            bronze,
            state,
            item.source_schema,
            item.table_name,
            item.primary_keys,
            fields,
        )
        plans.append((target, spark.sql(statement)))
    for target, frame in plans:
        write_snapshot(frame, target)
        print(f"Silver Delta atualizada: {target}")


def run_gold_statement(
    spark, config, name, statement, silver_tables, gold_dependencies=()
):
    """Executa uma única tabela Gold; o notebook é dono de sua consulta e dependências.

    Não consulta o manifesto nem o registro central de queries: novos objetivos
    podem usar qualquer subconjunto de tabelas Silver e seu próprio SQL.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    catalog, schema = config["catalog"], config["gold_schema"]
    source_schema = config.get("source_schema", "public")
    inputs = [
        (config["silver_schema"], source_name(source_schema, table))
        for table in silver_tables
    ]
    inputs.extend((schema, table) for table in gold_dependencies)
    assert_delta_targets(spark, catalog, inputs, require_exists=True)
    assert_delta_targets(spark, catalog, [(schema, name)])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace(catalog, schema)}")
    target = table_name(catalog, schema, name)
    write_snapshot(spark.sql(statement), target)
    print(f"Gold Delta atualizada: {target}")
