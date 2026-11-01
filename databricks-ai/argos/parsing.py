"""Parser BSON independente de Spark; valida sem substituir desconhecido por zero."""

import json
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import UUID
from .schemas import SCHEMAS, IDS


def typed_value(value, spec, path, errors):
    if value is None:
        return None
    if isinstance(spec, dict):
        if not isinstance(value, dict):
            errors.append(path + ":object")
            return None
        return {
            k: typed_value(value.get(k), v, path + "." + k, errors)
            for k, v in spec.items()
        }
    if isinstance(spec, list):
        if not isinstance(value, (list, tuple)):
            errors.append(path + ":array")
            return None
        if any(x is None for x in value):
            errors.append(path + ":null_element")
        return [typed_value(x, spec[0], path + "[]", errors) for x in value]
    try:
        if spec == "STRING":
            if isinstance(value, (str, UUID)):
                return str(value)
            if (
                path.endswith((".code", ".error_code"))
                and isinstance(value, int)
                and not isinstance(value, bool)
            ):
                return str(value)
            raise ValueError()
        if spec == "BIGINT":
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not -(2**63) <= value < 2**63
            ):
                raise ValueError()
            return int(value)
        if spec == "DOUBLE":
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float, Decimal))
                or not math.isfinite(float(value))
            ):
                raise ValueError()
            return float(value)
        if spec == "BOOLEAN":
            if not isinstance(value, bool):
                raise ValueError()
            return value
        if spec == "TIMESTAMP":
            if isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset().total_seconds() != 0
            ):
                raise ValueError()
            return value.astimezone(timezone.utc)
        if spec == "DATE":
            if isinstance(value, datetime):
                raise ValueError()
            return (
                date.fromisoformat(value)
                if isinstance(value, str)
                else (
                    value
                    if isinstance(value, date)
                    else (_ for _ in ()).throw(ValueError())
                )
            )
        if spec.startswith("DECIMAL"):
            from bson.decimal128 import Decimal128

            precision, scale = map(int, re.findall(r"\d+", spec))
            if isinstance(value, Decimal128):
                value = value.to_decimal()
            if not isinstance(value, (Decimal, str, int)) or isinstance(value, bool):
                raise ValueError()
            result = Decimal(value)
            if not result.is_finite():
                raise ValueError()
            # Falhar em vez de arredondar silenciosamente no Spark.
            exponent = result.as_tuple().exponent
            if max(0, -exponent) > scale or result.adjusted() + 1 > precision - scale:
                raise ValueError()
            return result
        if spec == "MAP<STRING,BIGINT>":
            if not isinstance(value, dict):
                raise ValueError()
            return {
                str(k): typed_value(v, "BIGINT", path + "." + str(k), errors)
                for k, v in value.items()
            }
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        errors.append(path + ":" + spec)
        return None
    raise ValueError("Tipo de schema não implementado: " + str(spec))


def require_uuid(record, key, errors, optional=False):
    value = record.get(key)
    if optional and value is None:
        return
    try:
        if not isinstance(value, str) or str(UUID(value)) != value.lower():
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        errors.append(key + ":uuid")


def parse_embedded(value):
    if not isinstance(value, str):
        return None
    try:
        result = json.loads(value)
        return result if isinstance(result, dict) else None
    except (ValueError, TypeError):
        return None


def prepare_document(alias, doc):
    doc = dict(doc)
    doc[IDS[alias]] = doc.get("_id")
    if alias == "email_operation_references" and isinstance(doc.get("_id"), str):
        doc[IDS[alias]] = doc["_id"].removeprefix("operation:")
    if alias == "interactions":
        doc["interaction_kind"] = doc.get("kind")
        doc["interaction_status"] = doc.get("status")
        result = parse_embedded(doc.get("result_json"))
        valid = (
            result is not None
            and result.get("schema_version") == "1"
            and result.get("status") in ("completed", "degraded")
            and isinstance(result.get("response"), dict)
            and result["response"].get("kind") in ("answer", "question")
            and isinstance(result.get("degradation_reasons", []), list)
            and all(isinstance(x, str) for x in result.get("degradation_reasons", []))
        )
        doc["result_parse_status"] = (
            "absent"
            if doc.get("result_json") is None
            else "valid" if valid else "invalid"
        )
        if valid:
            doc.update(
                result_status=result["status"],
                response_kind=result["response"]["kind"],
                question_id=result["response"].get("question_id"),
                degradation_reasons=result.get("degradation_reasons", []),
            )
    if alias == "messages":
        value = parse_embedded(doc.get("payload_json"))
        valid = value is not None
        doc["payload_parse_status"] = (
            "absent"
            if doc.get("payload_json") is None
            else "valid" if valid else "invalid"
        )
        if (
            doc.get("role") == "user"
            and doc.get("kind") == "user_input"
            and valid
            and value.get("kind") == "answer"
        ):
            doc["source_interaction_id"] = value.get("interaction_id")
            doc["answered_question_id"] = value.get("question_id")
    if alias == "execution_events":
        doc["judge_parse_status"] = "not_applicable"
        if (
            doc.get("target") == "agent"
            and doc.get("target_name") == "judge"
            and doc.get("status") != "started"
        ):
            result = (
                parse_embedded(doc.get("sanitized_output"))
                if doc.get("status") == "succeeded"
                else None
            )
            claims = result.get("unsupported_claims", []) if result else None
            valid = (
                result is not None
                and result.get("decision") in ("approved", "revise", "rejected")
                and isinstance(claims, list)
                and all(isinstance(x, str) for x in claims)
                and not (result.get("decision") == "approved" and claims)
            )
            doc["judge_parse_status"] = "valid" if valid else "invalid"
            if valid:
                doc.update(judge_decision=result["decision"], unsupported_claims=claims)
    return doc


def validate_record(alias, record, errors):
    require_uuid(record, IDS[alias], errors)
    dates = {
        "sessions": ("created_at", "updated_at"),
        "interactions": ("created_at", "updated_at"),
        "messages": ("created_at",),
        "execution_events": ("created_at",),
        "session_summaries": ("updated_at",),
        "user_memory": ("updated_at",),
        "report_artifacts": ("created_at", "expires_at", "purge_at"),
        "email_preparations": ("created_at", "expires_at", "purge_at"),
        "pending_recipient_resolutions": ("expires_at", "purge_at"),
        "email_authorizations": ("authorized_at", "expires_at", "purge_at"),
        "email_operation_references": ("recorded_at", "purge_at"),
        "email_reports": ("created_at", "expires_at"),
        "email_dispatches": ("created_at", "updated_at"),
    }
    for key in dates.get(alias, ()):
        if record.get(key) is None:
            errors.append(key + ":required")
    for key in (
        "session_id",
        "interaction_id",
        "user_id",
        "principal_user_id",
        "invocation_id",
    ):
        if key in record and key != IDS[alias]:
            require_uuid(record, key, errors, optional=alias == "observations")
    if alias == "observations":
        required = (
            "schema_version",
            "service",
            "environment",
            "operation_kind",
            "operation_name",
            "observation_level",
            "execution_mode",
            "started_at",
            "updated_at",
            "status",
        )
        for key in required:
            if record.get(key) is None or record.get(key) == "":
                errors.append(key + ":required")
        enums = {
            "schema_version": {"1", "2"},
            "status": {
                "started",
                "succeeded",
                "failed",
                "timeout",
                "cancelled",
                "rejected",
            },
            "operation_kind": {
                "http",
                "interaction",
                "graph",
                "agent",
                "model",
                "embedding",
                "tool",
                "mcp",
                "a2a",
                "workflow",
                "storage",
                "external",
            },
            "observation_level": {
                "local_operation",
                "sdk_invocation",
                "transport_attempt",
            },
            "execution_mode": {"real", "mock", "unavailable"},
            "operation_phase": {
                "initialization",
                "execution",
                "validation",
                "recovery",
            },
            "attempt_reason": {
                "initial",
                "retry_transient",
                "fallback",
                "repair",
                "judge_revision",
                "source_renewal",
                "unknown",
            },
            "attempt_visibility": {"individual", "internal_retries_unknown"},
            "cancellation_state": {"requested", "observed", "confirmed"},
        }
        for key, values in enums.items():
            if record.get(key) is not None and record[key] not in values:
                errors.append(key + ":enum")
        for key in ("correlation_id", "parent_observation_id"):
            require_uuid(record, key, errors, optional=True)
        terminal = record.get("status") not in (None, "started")
        if terminal and (
            record.get("ended_at") is None or record.get("duration_ms") is None
        ):
            errors.append("terminal:missing_end")
        if record.get("status") == "started" and (
            record.get("ended_at") is not None or record.get("duration_ms") is not None
        ):
            errors.append("started:has_end")
        if (
            record.get("status") in ("failed", "timeout")
            and record.get("error") is None
        ):
            errors.append("failure:missing_error")
        usage = record.get("usage")
        if usage:
            if usage["availability"] not in (
                "returned",
                "partial",
                "absent",
                "not_applicable",
            ):
                errors.append("usage.availability:enum")
            counts = [
                v for k, v in usage.items() if k.endswith("_tokens") and v is not None
            ]
            if any(v < 0 for v in counts) or any(
                v is None or v < 0 for v in (usage.get("details") or {}).values()
            ):
                errors.append("usage:negative")
            if usage["availability"] == "absent" and (counts or usage.get("details")):
                errors.append("usage.absent:has_counts")
        billing = record.get("billing")
        if billing and billing.get("reported_cost") is not None:
            if (
                billing["reported_cost"] < 0
                or not re.fullmatch("[A-Z]{3}", billing.get("currency") or "")
                or billing.get("cost_source") not in ("provider", "configured_rate")
            ):
                errors.append("billing:invalid")
        if (
            billing
            and billing.get("reported_cost") is None
            and (
                billing.get("currency") is not None
                or billing.get("cost_source") is not None
            )
        ):
            errors.append("billing:metadata_without_cost")
        error = record.get("error")
        if error and (
            not error.get("type")
            or error.get("origin")
            not in (
                "application",
                "provider",
                "transport",
                "protocol",
                "validation",
                "storage",
                "unknown",
            )
        ):
            errors.append("error:invalid")
        http = record.get("http")
        if (
            http
            and http.get("status_code") is not None
            and not 100 <= http["status_code"] <= 599
        ):
            errors.append("http.status_code:range")
        for key in ("duration_ms", "timeout_ms"):
            if record.get(key) is not None and (
                record[key] < 0 or (key == "timeout_ms" and record[key] == 0)
            ):
                errors.append(key + ":negative")
        if record.get("attempt_number") is not None and record["attempt_number"] < 1:
            errors.append("attempt_number:positive")
        for resource in record.get("resources") or []:
            if resource and (
                resource.get("quantity") is None
                or resource["quantity"] < 0
                or not resource.get("name")
                or not resource.get("unit")
                or resource.get("source") not in ("provider", "sdk", "application")
            ):
                errors.append("resources:invalid")
        if (
            record.get("updated_at")
            and record.get("ended_at")
            and record["updated_at"] < record["ended_at"]
        ):
            errors.append("updated_at:before_end")
    if alias == "sessions":
        farm_ids = record.get("farm_ids")
        if not farm_ids or len(farm_ids) != len(set(farm_ids)):
            errors.append("farm_ids:empty_or_duplicate")
        for value in farm_ids or []:
            require_uuid({"farm": value}, "farm", errors)
    if alias == "interactions":
        if record["interaction_kind"] not in ("message", "answer"):
            errors.append("interaction_kind:enum")
        if record["interaction_status"] not in (
            "accepted",
            "running",
            "completed",
            "degraded",
            "failed",
            "interrupted",
        ):
            errors.append("interaction_status:enum")
    if alias == "execution_events":
        if record["status"] not in ("started", "succeeded", "failed") or record[
            "target"
        ] not in ("agent", "tool"):
            errors.append("execution:enum")
        if record["status"] != "started" and record.get("completed_at") is None:
            errors.append("execution:missing_end")
        if record["status"] == "failed" and not record.get("safe_error_code"):
            errors.append("execution:missing_error")
        if record["status"] == "started" and record.get("completed_at") is not None:
            errors.append("execution:started_has_end")
    if alias == "messages":
        if record["role"] not in ("user", "assistant") or record["kind"] not in (
            "user_input",
            "assistant_answer",
            "clarification_question",
        ):
            errors.append("message:enum")
    if alias == "email_dispatches" and record["state"] not in (
        "pending",
        "accepted",
        "partial",
        "failed",
        "unknown",
    ):
        errors.append("state:enum")
    for key in ("sequence", "version", "revision", "attempt"):
        if key in record and record[key] is not None and record[key] < 1:
            errors.append(key + ":positive")
    for start, end in (
        ("started_at", "ended_at"),
        ("created_at", "updated_at"),
        ("created_at", "completed_at"),
    ):
        if record.get(start) and record.get(end) and record[end] < record[start]:
            errors.append(end + ":before_start")


def parse_record(alias, raw):
    from bson import json_util

    alias = "observations" if alias == "a2a_observations" else alias
    errors = []
    try:
        doc = json_util.loads(
            raw,
            json_options=json_util.CANONICAL_JSON_OPTIONS.with_options(tz_aware=True),
        )
        if not isinstance(doc, dict):
            raise ValueError()
        doc = prepare_document(alias, doc)
        record = typed_value(doc, SCHEMAS[alias], alias, errors)
        validate_record(alias, record, errors)
        # Hash sem updated_at permite identificar dois terminais semanticamente divergentes.
        import hashlib

        semantic = dict(doc)
        semantic.pop("updated_at", None)
        semantic_hash = hashlib.sha256(
            json_util.dumps(
                semantic,
                json_options=json_util.CANONICAL_JSON_OPTIONS,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return {
            "record": record,
            "errors": sorted(set(errors)),
            "semantic_hash": semantic_hash,
        }
    except Exception:
        # Só códigos de regra, sem texto que possa conter conteúdo/credenciais.
        return {
            "record": None,
            "errors": ["document:unparseable"],
            "semantic_hash": None,
        }
