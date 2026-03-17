from utils.utils import *
import streamlit as st
from datetime import date, datetime, time, timedelta
import json
from typing import Any
from urllib.parse import urlparse


from dotenv import load_dotenv
load_dotenv()

def _read_with_aliases(*names: str, default: str = "") -> str:
    for name in names:
        value = read_config(name, "")
        if value:
            return value
    return default


def _load_mysql_config() -> dict[str, str | int | dict[str, str]]:
    config = {
        "host": _read_with_aliases("MYSQL_HOST", "DB_HOST"),
        "port": int(_read_with_aliases("MYSQL_PORT", "DB_PORT", default="3306")),
        "user": _read_with_aliases("MYSQL_USERNAME", "MYSQL_USER", "DB_USERNAME", "DB_USER"),
        "password": _read_with_aliases("MYSQL_PASSWORD", "MYSQL_PASS", "DB_PASSWORD"),
        "database": _read_with_aliases("MYSQL_DATABASE", "MYSQL_DB", "DB_DATABASE", "DB_NAME"),
    }

    ssl_ca = _read_with_aliases("MYSQL_SSL_CA")
    if ssl_ca:
        config["ssl"] = {"ca": ssl_ca}

    if not config["host"]:
        _apply_mysql_url_fallback(config)

    return config


def _apply_mysql_url_fallback(config: dict[str, str | int | dict[str, str]]) -> None:
    """Load DB connection fields from URL-like envs when split fields are not provided."""
    raw_url = _read_with_aliases("MYSQL_URL", "DATABASE_URL", "DB_URL")
    if not raw_url:
        return

    parsed = urlparse(raw_url)
    if parsed.scheme not in ("mysql", "mysql+pymysql"):
        return

    if parsed.hostname:
        config["host"] = parsed.hostname
    if parsed.port:
        config["port"] = int(parsed.port)
    if parsed.username:
        config["user"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password
    if parsed.path and parsed.path != "/":
        config["database"] = parsed.path.lstrip("/")


def _is_safe_identifier(value: str) -> bool:
    return bool(value) and value.replace("_", "").isalnum()


def _resolve_waybill_table(conn: Any) -> str:
    """Pick an existing waybill table, with env override support."""
    config = _load_mysql_config()
    schema = str(config["database"])
    preferred = _read_with_aliases("WAYBILL_TABLE", "MYSQL_WAYBILL_TABLE")

    candidates = [
        preferred,
        "waybill_waybills",
        "waybills",
        "waybill",
    ]
    candidates = [name.strip() for name in candidates if str(name).strip()]

    with conn.cursor() as cur:
        existing_tables: set[str] = set()

        try:
            cur.execute("SHOW TABLES")
            for row in cur.fetchall() or []:
                if not isinstance(row, dict):
                    continue
                for value in row.values():
                    table_name = str(value or "").strip()
                    if table_name:
                        existing_tables.add(table_name)
        except Exception:
            existing_tables = set()

        if not existing_tables:
            try:
                if candidates:
                    placeholders = ", ".join(["%s"] * len(candidates))
                    cur.execute(
                        f"""
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s
                            AND table_name IN ({placeholders})
                        """,
                        [schema, *candidates],
                    )
                    existing_tables = {str(row.get("table_name") or "") for row in cur.fetchall()}
            except Exception:
                existing_tables = set()

        for candidate in candidates:
            if candidate in existing_tables:
                return candidate

        if existing_tables:
            fuzzy_candidates = sorted(table for table in existing_tables if "waybill" in table)
            if fuzzy_candidates:
                return fuzzy_candidates[0]

        for candidate in candidates:
            if not _is_safe_identifier(candidate):
                continue
            try:
                cur.execute(f"SELECT 1 FROM {candidate} LIMIT 1")
                return candidate
            except Exception:
                continue

    return ""


def _resolve_router_messages_table(conn: Any) -> str:
    """Pick an existing router_messages cache table, with env override support."""
    config = _load_mysql_config()
    schema = str(config["database"])
    preferred = _read_with_aliases("ROUTER_MESSAGES_TABLE", "MYSQL_ROUTER_MESSAGES_TABLE")

    candidates = [
        preferred,
        "third_party_transit_cache",
        "transit_third_party_cache",
        "third_party_cache",
        "transit_router_messages_cache",
    ]
    candidates = [name.strip() for name in candidates if str(name).strip()]

    with conn.cursor() as cur:
        existing_tables: set[str] = set()

        # Prefer SHOW TABLES: some DB users have restricted access to information_schema.
        try:
            cur.execute("SHOW TABLES")
            for row in cur.fetchall() or []:
                if not isinstance(row, dict):
                    continue
                for value in row.values():
                    table_name = str(value or "").strip()
                    if table_name:
                        existing_tables.add(table_name)
        except Exception:
            existing_tables = set()

        if not existing_tables:
            try:
                if candidates:
                    placeholders = ", ".join(["%s"] * len(candidates))
                    cur.execute(
                        f"""
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s
                            AND table_name IN ({placeholders})
                        """,
                        [schema, *candidates],
                    )
                    existing_tables = {str(row.get("table_name") or "") for row in cur.fetchall()}
            except Exception:
                existing_tables = set()

        for candidate in candidates:
            if candidate in existing_tables:
                return candidate

        if existing_tables:
            fuzzy_candidates = sorted(table for table in existing_tables if "third_party_cache" in table)
            if fuzzy_candidates:
                return fuzzy_candidates[0]

        # Last-resort lookup when metadata queries are restricted.
        for candidate in candidates:
            if not candidate.replace("_", "").isalnum():
                continue
            try:
                cur.execute(f"SELECT 1 FROM {candidate} LIMIT 1")
                return candidate
            except Exception:
                continue

    return ""


def _load_table_columns(conn: Any, table_name: str) -> set[str]:
    """Best-effort column discovery for a table."""
    columns: set[str] = set()
    if not _is_safe_identifier(table_name):
        return columns

    with conn.cursor() as cur:
        try:
            cur.execute(f"SHOW COLUMNS FROM {table_name}")
            for row in cur.fetchall() or []:
                if not isinstance(row, dict):
                    continue
                field_name = str(row.get("Field") or "").strip()
                if field_name:
                    columns.add(field_name)
            if columns:
                return columns
        except Exception:
            columns = set()

        try:
            cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
            if hasattr(cur, "description") and cur.description:
                columns = {str(desc[0]).strip() for desc in cur.description if desc and str(desc[0]).strip()}
        except Exception:
            columns = set()

    return columns


def _resolve_router_messages_order_column(columns: set[str]) -> str:
    """Pick the best ordering column from router-message cache table columns."""
    preferred = (
        "created_at",
        "updated_at",
        "event_time",
        "sync_time",
        "id",
    )
    for candidate in preferred:
        if candidate in columns:
            return candidate
    return ""


def _resolve_column(columns: set[str], env_names: tuple[str, ...], candidates: tuple[str, ...]) -> str:
    preferred = _read_with_aliases(*env_names)
    if preferred and preferred in columns:
        return preferred

    for candidate in candidates:
        if candidate in columns:
            return candidate
    return ""

DB_FETCH_BATCH_SIZE = max(100, int(read_config("DB_FETCH_BATCH_SIZE", "5000")))

def _require_db_env() -> None:
    config = _load_mysql_config()
    missing = []
    if not config["host"]:
        missing.append("MYSQL_HOST / DATABASE_URL")
    if not config["user"]:
        missing.append("MYSQL_USERNAME / DATABASE_URL")
    if not config["password"]:
        missing.append("MYSQL_PASSWORD / DATABASE_URL")
    if not config["database"]:
        missing.append("MYSQL_DATABASE / DATABASE_URL")
    if missing:
        raise RuntimeError(f"MySQL 环境变量未配置：{', '.join(missing)}")


@st.cache_data(ttl=60, show_spinner=False)
def fetch_tracking_numbers_by_date(start_date: date, end_date: date) -> list[str]:
    # fake tracking number for testing
    # return ["ZX34043383"]

    """
    Query waybill_waybills for tracking_number where created_at is between
    [start_date 00:00:00, end_date 23:59:59.999999] (inclusive by date).
    """
    _require_db_env()

    # lazy import so the app can still run without DB deps until this mode is used
    try:
        import pymysql  # type: ignore
    except Exception as e:
        raise RuntimeError("缺少依赖 pymysql。请先 pip install pymysql") from e

    if end_date < start_date:
        return []

    start_dt = datetime.combine(start_date, time.min)
    # Use an exclusive upper-bound at next-day 00:00:00 to avoid dropping rows on end_date.
    end_exclusive_dt = datetime.combine(end_date + timedelta(days=1), time.min)

    config = _load_mysql_config()
    conn = pymysql.connect(
        host=str(config["host"]),
        port=int(config["port"]),
        user=str(config["user"]),
        password=str(config["password"]),
        database=str(config["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    try:
        table_name = _resolve_waybill_table(conn)
        if not _is_safe_identifier(table_name):
            return []

        table_columns = _load_table_columns(conn, table_name)
        if not table_columns:
            return []

        tracking_column = _resolve_column(
            table_columns,
            ("WAYBILL_TRACKING_COLUMN",),
            ("tracking_number", "tracking_id", "waybill_no"),
        )
        created_at_column = _resolve_column(
            table_columns,
            ("WAYBILL_CREATED_AT_COLUMN",),
            ("created_at", "create_time", "created_time", "gmt_create"),
        )
        if not tracking_column or not created_at_column:
            return []

        with conn.cursor() as cur:
            sql = f"""
                SELECT DISTINCT {tracking_column} AS tracking_number
                FROM {table_name}
                WHERE {created_at_column} >= %s AND {created_at_column} < %s
                AND {tracking_column} IS NOT NULL AND {tracking_column} <> ''
                ORDER BY {tracking_column} ASC
            """
            cur.execute(sql, (start_dt, end_exclusive_dt))

            tracking_numbers: list[str] = []
            while True:
                rows = cur.fetchmany(DB_FETCH_BATCH_SIZE)
                if not rows:
                    break
                tracking_numbers.extend(str(r["tracking_number"]).strip() for r in rows if r.get("tracking_number"))
            return tracking_numbers
    finally:
        conn.close()

@st.cache_data(ttl=60, show_spinner=False)
def fetch_tracking_numbers_by_delivery_window(start_date: date, end_date: date) -> list[str]:
    """
    Query waybill_waybills for tracking_number where created_at is between
    [start_date - 7 days, end_date] inclusive.
    """
    shifted_start = start_date - timedelta(days=7)
    return fetch_tracking_numbers_by_date(shifted_start, end_date)



@st.cache_data(ttl=60, show_spinner=False)
def fetch_receive_province_map(tracking_ids: tuple[str, ...]) -> dict[str, str]:
    """
    Query waybill_waybills.receive_province by tracking_number for given tracking_ids.
    """
    _require_db_env()

    try:
        import pymysql  # type: ignore
    except Exception as e:
        raise RuntimeError("Missing dependency: pymysql. Please run: pip install pymysql") from e

    if not tracking_ids:
        return {}

    tracking_ids_clean = tuple(str(tid).strip() for tid in tracking_ids if str(tid).strip())
    if not tracking_ids_clean:
        return {}

    config = _load_mysql_config()
    conn = pymysql.connect(
        host=str(config["host"]),
        port=int(config["port"]),
        user=str(config["user"]),
        password=str(config["password"]),
        database=str(config["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    receive_province_map: dict[str, str] = {}
    try:
        table_name = _resolve_waybill_table(conn)
        if not _is_safe_identifier(table_name):
            return {}

        table_columns = _load_table_columns(conn, table_name)
        if not table_columns:
            return {}

        tracking_column = _resolve_column(
            table_columns,
            ("WAYBILL_TRACKING_COLUMN",),
            ("tracking_number", "tracking_id", "waybill_no"),
        )
        receive_province_column = _resolve_column(
            table_columns,
            ("WAYBILL_RECEIVE_PROVINCE_COLUMN",),
            ("receive_province", "receiver_province", "consignee_province"),
        )
        if not tracking_column or not receive_province_column:
            return {}

        with conn.cursor() as cur:
            chunk_size = 500
            for i in range(0, len(tracking_ids_clean), chunk_size):
                chunk = tracking_ids_clean[i : i + chunk_size]
                placeholders = ", ".join(["%s"] * len(chunk))
                sql = f"""
                    SELECT {tracking_column} AS tracking_number,
                           {receive_province_column} AS receive_province
                    FROM {table_name}
                    WHERE {tracking_column} IN ({placeholders})
                """
                cur.execute(sql, chunk)
                while True:
                    rows = cur.fetchmany(DB_FETCH_BATCH_SIZE)
                    if not rows:
                        break
                    for row in rows:
                        tracking_number = str(row.get("tracking_number") or "").strip()
                        if not tracking_number:
                            continue
                        receive_province_map[tracking_number] = str(row.get("receive_province") or "").strip()
    finally:
        conn.close()

    return receive_province_map

@st.cache_data(ttl=60, show_spinner=False)
def fetch_sender_info_map(tracking_ids: tuple[str, ...]) -> dict[str, dict[str, str]]:
    """
    Query sender fields from waybill_waybills for given tracking_ids.
    """
    _require_db_env()

    try:
        import pymysql  # type: ignore
    except Exception as e:
        raise RuntimeError("Missing dependency: pymysql. Please run: pip install pymysql") from e

    if not tracking_ids:
        return {}

    tracking_ids_clean = tuple(str(tid).strip() for tid in tracking_ids if str(tid).strip())
    if not tracking_ids_clean:
        return {}

    config = _load_mysql_config()
    conn = pymysql.connect(
        host=str(config["host"]),
        port=int(config["port"]),
        user=str(config["user"]),
        password=str(config["password"]),
        database=str(config["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    sender_info_map: dict[str, dict[str, str]] = {}
    try:
        table_name = _resolve_waybill_table(conn)
        if not _is_safe_identifier(table_name):
            return {}

        table_columns = _load_table_columns(conn, table_name)
        if not table_columns:
            return {}

        tracking_column = _resolve_column(
            table_columns,
            ("WAYBILL_TRACKING_COLUMN",),
            ("tracking_number", "tracking_id", "waybill_no"),
        )
        sender_company_column = _resolve_column(
            table_columns,
            ("WAYBILL_SENDER_COMPANY_COLUMN",),
            ("sender_company", "shipper_company", "sender_name"),
        )
        sender_province_column = _resolve_column(
            table_columns,
            ("WAYBILL_SENDER_PROVINCE_COLUMN",),
            ("sender_province", "shipper_province"),
        )
        sender_city_column = _resolve_column(
            table_columns,
            ("WAYBILL_SENDER_CITY_COLUMN",),
            ("sender_city", "shipper_city"),
        )
        sender_address_column = _resolve_column(
            table_columns,
            ("WAYBILL_SENDER_ADDRESS_COLUMN",),
            ("sender_address", "shipper_address", "sender_addr"),
        )

        if (
            not tracking_column
            or not sender_company_column
            or not sender_province_column
            or not sender_city_column
            or not sender_address_column
        ):
            return {}

        with conn.cursor() as cur:
            chunk_size = 500
            for i in range(0, len(tracking_ids_clean), chunk_size):
                chunk = tracking_ids_clean[i : i + chunk_size]
                placeholders = ", ".join(["%s"] * len(chunk))
                sql = f"""
                    SELECT {tracking_column} AS tracking_number,
                           {sender_company_column} AS sender_company,
                           {sender_province_column} AS sender_province,
                           {sender_city_column} AS sender_city,
                           {sender_address_column} AS sender_address
                    FROM {table_name}
                    WHERE {tracking_column} IN ({placeholders})
                """
                cur.execute(sql, chunk)
                while True:
                    rows = cur.fetchmany(DB_FETCH_BATCH_SIZE)
                    if not rows:
                        break
                    for row in rows:
                        tracking_number = str(row.get("tracking_number") or "").strip()
                        if not tracking_number:
                            continue
                        sender_info_map[tracking_number] = {
                            "sender_company": str(row.get("sender_company") or "").strip(),
                            "sender_province": str(row.get("sender_province") or "").strip(),
                            "sender_city": str(row.get("sender_city") or "").strip(),
                            "sender_address": str(row.get("sender_address") or "").strip(),
                        }
    finally:
        conn.close()

    return sender_info_map


@st.cache_data(ttl=60, show_spinner=False)
def fetch_router_messages_map(tracking_ids: tuple[str, ...]) -> dict[str, Any]:
    """Load latest cached router_messages JSON by tracking_number."""
    _require_db_env()

    try:
        import pymysql  # type: ignore
    except Exception as e:
        raise RuntimeError("Missing dependency: pymysql. Please run: pip install pymysql") from e

    if not tracking_ids:
        return {}

    tracking_ids_clean = tuple(str(tid).strip() for tid in tracking_ids if str(tid).strip())
    if not tracking_ids_clean:
        return {}

    config = _load_mysql_config()
    conn = pymysql.connect(
        host=str(config["host"]),
        port=int(config["port"]),
        user=str(config["user"]),
        password=str(config["password"]),
        database=str(config["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    payload_map: dict[str, Any] = {}
    try:
        table_name = _resolve_router_messages_table(conn)
        if not table_name:
            return {}
        if not _is_safe_identifier(table_name):
            return {}

        table_columns = _load_table_columns(conn, table_name)
        if not table_columns:
            return {}

        if "tracking_number" not in table_columns or "router_messages" not in table_columns:
            return {}

        order_column = _resolve_router_messages_order_column(table_columns)
        order_sql = f"ORDER BY tracking_number ASC, {order_column} DESC" if order_column else "ORDER BY tracking_number ASC"

        with conn.cursor() as cur:
            chunk_size = 500
            for i in range(0, len(tracking_ids_clean), chunk_size):
                chunk = tracking_ids_clean[i : i + chunk_size]
                placeholders = ", ".join(["%s"] * len(chunk))
                sql = f"""
                    SELECT tracking_number, router_messages
                    FROM {table_name}
                    WHERE tracking_number IN ({placeholders})
                    AND router_messages IS NOT NULL
                    {order_sql}
                """
                cur.execute(sql, chunk)

                while True:
                    rows = cur.fetchmany(DB_FETCH_BATCH_SIZE)
                    if not rows:
                        break
                    for row in rows:
                        tracking_number = str(row.get("tracking_number") or "").strip()
                        if not tracking_number or tracking_number in payload_map:
                            continue

                        raw_payload = row.get("router_messages")
                        if raw_payload is None:
                            continue

                        if isinstance(raw_payload, (dict, list)):
                            payload_map[tracking_number] = raw_payload
                            continue

                        text_payload = str(raw_payload).strip()
                        if not text_payload:
                            continue

                        try:
                            payload_map[tracking_number] = json.loads(text_payload)
                        except json.JSONDecodeError:
                            # Keep as raw text so upper layer can report parse failure.
                            payload_map[tracking_number] = text_payload
    finally:
        conn.close()

    return payload_map


def clear_query_caches() -> None:
    """Clear all DB query caches so users can fetch the latest updated records immediately."""
    for fn in (
        fetch_tracking_numbers_by_date,
        fetch_tracking_numbers_by_delivery_window,
        fetch_receive_province_map,
        fetch_sender_info_map,
        fetch_router_messages_map,
    ):
        fn.clear()
