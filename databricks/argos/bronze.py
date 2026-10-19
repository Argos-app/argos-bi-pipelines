"""Adaptação do loop de ingestão fornecido pelo usuário."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pyspark.sql import functions as F

from argos.common import (
    build_jdbc_options,
    event_expression,
    ingestion_mode,
    pg_ident,
    pg_projection,
    source_name,
    table_name,
)


def ingest(spark, config):
    options = build_jdbc_options(config)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    lookback = int(config["lookback_hours"])
    if lookback < 0:
        raise ValueError("lookback_hours deve ser >= 0")

    def query(sql):
        return (
            spark.read.format("jdbc")
            .options(**options)
            .option("dbtable", f"({sql}) AS consulta")
            .load()
        )

    catalog, schema = config["catalog"], config["bronze_schema"]
    for target_schema in (schema, config["silver_schema"], config["gold_schema"]):
        spark.sql(
            f'CREATE SCHEMA IF NOT EXISTS {table_name(catalog, target_schema, "x").rsplit(".", 1)[0]}'
        )
    state = table_name(catalog, schema, "_ingestion_state")
    manifest = table_name(catalog, schema, "_table_manifest")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {state} (
        source_schema STRING, table_name STRING, batch_id STRING,
        mode STRING, completed_at TIMESTAMP
    ) USING DELTA""")
    source_schema = config["source_schema"].replace("'", "''")
    # Sem HAVING: inclui permission, user_property, notification_delivery etc.
    columns = query(
        f"""SELECT t.table_schema, t.table_name, c.column_name, c.ordinal_position, c.data_type
        FROM information_schema.tables t
        JOIN information_schema.columns c USING (table_catalog, table_schema, table_name)
        WHERE t.table_type = 'BASE TABLE' AND t.table_schema = '{source_schema}'
        ORDER BY t.table_name, c.ordinal_position"""
    ).collect()
    tables = defaultdict(list)
    source_types = defaultdict(dict)
    for row in columns:
        tables[(row.table_schema, row.table_name)].append(row.column_name)
        source_types[(row.table_schema, row.table_name)][row.column_name] = row.data_type
    if not tables:
        raise ValueError("Nenhuma tabela encontrada no schema de origem")
    if spark.catalog.tableExists(manifest):
        previous = {
            (row.source_schema, row.table_name)
            for row in spark.table(manifest).collect()
        }
        removed = previous - set(tables)
        if removed:
            # Evita publicar um inventário incompleto quando tabelas da origem deixam de estar visíveis.
            raise ValueError(
                f"Tabelas antes ingeridas não foram encontradas: {sorted(removed)}. "
                "Confira a conexão, as permissões e o schema da origem antes de continuar."
            )
    pk_rows = query(f"""SELECT tc.table_schema, tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_catalog = kcu.constraint_catalog
         AND tc.constraint_schema = kcu.constraint_schema
         AND tc.constraint_name = kcu.constraint_name
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = '{source_schema}'
        ORDER BY tc.table_name, kcu.ordinal_position""").collect()
    pks = defaultdict(list)
    for row in pk_rows:
        pks[(row.table_schema, row.table_name)].append(row.column_name)
    metadata = []
    for (origin_schema, name), source_columns in sorted(tables.items()):
        reserved = [c for c in source_columns if c.startswith("_")]
        if reserved:
            raise ValueError(f"{name}: colunas reservadas na origem: {reserved}")
        mode = ingestion_mode(
            source_columns, config["force_snapshot"].lower() == "true"
        )
        target = table_name(catalog, schema, source_name(origin_schema, name))
        origin = f"{pg_ident(origin_schema)}.{pg_ident(name)}"
        event = event_expression(source_columns)
        exists = spark.catalog.tableExists(target)
        if exists and mode == "incremental":
            required_metadata = {
                "_change_at",
                "_row_hash",
                "_batch_id",
                "_ingested_at",
                "_ingestion_mode",
            }
            missing_metadata = required_metadata - set(spark.table(target).columns)
            if missing_metadata:
                raise ValueError(
                    f"{target}: Destino incompatível: faltam metadados {sorted(missing_metadata)}. "
                    "Use um schema de destino inicializado por este projeto."
                )
        cutoff = None
        if exists and mode == "incremental":
            watermark = (
                spark.table(target)
                .agg(F.max("_change_at").alias("watermark"))
                .first()
                .watermark
            )
            if watermark is not None:
                cutoff = watermark - timedelta(hours=lookback)
        # Inclui timestamps nulos; não perde registros da primeira carga.
        where = ""
        if cutoff is not None:
            formatted = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")
            where = f"WHERE {event} >= TIMESTAMP '{formatted}' OR {event} IS NULL"
        projection = pg_projection(source_columns, source_types[(origin_schema, name)])
        frame = query(f"SELECT {projection}, {event} AS _change_at FROM {origin} {where}")
        # Conteúdo faz parte da identidade da versão: timestamp igual pode ter UPDATE.
        frame = frame.withColumn(
            "_row_hash",
            F.sha2(
                F.to_json(
                    F.struct(*[F.col(c) for c in source_columns]),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
        if exists and mode == "incremental":
            old = spark.table(target)
            if cutoff is not None:
                old = old.where(
                    (F.col("_change_at") >= F.lit(cutoff))
                    | F.col("_change_at").isNull()
                )
            comparison = list(pks[(origin_schema, name)]) + ["_change_at", "_row_hash"]
            old = old.select(*comparison).dropDuplicates()
            condition = None
            for column in comparison:
                term = F.col(f"n.{column}").eqNullSafe(F.col(f"o.{column}"))
                condition = term if condition is None else condition & term
            frame = (
                frame.alias("n")
                .join(old.alias("o"), condition, "left_anti")
                .select("n.*")
            )
        batch_id = str(uuid4())
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        frame = (
            frame.withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.lit(ingested_at))
            .withColumn("_ingestion_mode", F.lit(mode))
        )
        frame.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(target)
        # Marcador depois do append. Snapshot vazio também publica um marcador.
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        spark.createDataFrame(
            [(origin_schema, name, batch_id, mode, completed_at)],
            "source_schema string, table_name string, batch_id string, mode string, completed_at timestamp",
        ).write.format("delta").mode("append").saveAsTable(state)
        metadata.append((origin_schema, name, pks[(origin_schema, name)]))
        print(f"Append concluído: {target} ({mode})")
    # Publica só depois de todas as tabelas concluírem. Job: max_concurrent_runs=1.
    spark.createDataFrame(
        metadata, "source_schema string, table_name string, primary_keys array<string>"
    ).write.format("delta").mode("overwrite").saveAsTable(manifest)
