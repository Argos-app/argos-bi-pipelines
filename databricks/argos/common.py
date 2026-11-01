"""Identificadores e políticas compartilhados; não depende de Spark."""


def pg_ident(value):
    return '"' + value.replace('"', '""') + '"'


def sql_ident(value):
    return "`" + value.replace("`", "``") + "`"


def table_name(catalog, schema, name):
    return ".".join(sql_ident(v) for v in (catalog, schema, name))


def source_name(schema, name):
    return schema + "__" + name


def ingestion_mode(columns, force_snapshot=False):

    return (
        "incremental" if "updated_at" in columns and not force_snapshot else "snapshot"
    )


def event_expression(columns):
    timestamps = [pg_ident(c) for c in ("created_at", "updated_at") if c in columns]
    if len(timestamps) == 2:
        return "GREATEST(" + ", ".join(timestamps) + ")"
    return timestamps[0] if timestamps else "NULL::timestamp"


def build_jdbc_options(config):
    """Lê credenciais diretas do notebook sem transformar ou imprimir seus valores."""
    from urllib.parse import urlsplit

    url = config.get("jdbc_url", "")
    if not url.startswith("jdbc:postgresql://"):
        raise ValueError("jdbc_url deve começar com jdbc:postgresql://")
    parsed = urlsplit(url[len("jdbc:") :])
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            "Use URL JDBC sem credenciais; preencha jdbc_user e jdbc_password no notebook."
        )
    user = config.get("jdbc_user", "")
    password = config.get("jdbc_password", "")
    if not user or not password or password == "PREENCHA_A_SENHA_DO_BANCO":
        raise ValueError(
            "Preencha jdbc_user e jdbc_password na célula de conexão do notebook Bronze."
        )
    return {
        "url": url,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }


def is_time_type(type_name):
    name = type_name.strip().lower()
    return name in {
        "time",
        "timetz",
        "time without time zone",
        "time with time zone",
    } or name.startswith("time(")


def pg_projection(columns, data_types):
    """Representa TIME como texto na origem, preservando nulos e frações."""
    expressions = []
    for column in columns:
        identifier = pg_ident(column)
        if is_time_type(data_types.get(column, "")):
            expressions.append(f"CAST({identifier} AS TEXT) AS {identifier}")
        else:
            expressions.append(identifier)
    return ", ".join(expressions)
