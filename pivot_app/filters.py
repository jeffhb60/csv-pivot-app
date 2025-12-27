from typing import Dict, Any, List

from .sql_utils import q_ident, sql_str, normalize_duckdb_type

ALLOWED_OPS = {
    "=", "!=", ">", ">=", "<", "<=",
    "contains", "startswith", "endswith",
    "is_null", "not_null",
}

NUMERIC_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "REAL", "FLOAT", "DOUBLE", "DECIMAL", "UBIGINT"}
DATE_TYPES = {"DATE"}
TIME_TYPES = {"TIME"}
TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS", "TIMESTAMP_TZ"}
BOOL_TYPES = {"BOOLEAN"}


def build_where(filters: List[Dict[str, Any]], col_types: Dict[str, str]) -> str:
    """Build WHERE clause from list filters: {'col','op','value'}."""
    clauses: List[str] = []

    for f in filters:
        col = f.get("col", "")
        op = f.get("op", "=")
        val = f.get("value", "")

        if not col:
            continue
        if op not in ALLOWED_OPS:
            raise ValueError(f"Unsupported operator: {op}")

        c = q_ident(col)
        t = normalize_duckdb_type(col_types.get(col, "VARCHAR"))

        if op == "is_null":
            clauses.append(f"{c} IS NULL")
            continue
        if op == "not_null":
            clauses.append(f"{c} IS NOT NULL")
            continue

        if op in {"contains", "startswith", "endswith"}:
            c_txt = f"CAST({c} AS VARCHAR)"
            if op == "contains":
                clauses.append(f"{c_txt} ILIKE '%' || {sql_str(val)} || '%'")
            elif op == "startswith":
                clauses.append(f"{c_txt} ILIKE {sql_str(str(val) + '%')}")
            else:
                clauses.append(f"{c_txt} ILIKE {sql_str('%' + str(val))}")
            continue

        if t in NUMERIC_TYPES:
            clauses.append(f"TRY_CAST({c} AS DOUBLE) {op} TRY_CAST({sql_str(val)} AS DOUBLE)")
        elif t in DATE_TYPES:
            clauses.append(f"TRY_CAST({c} AS DATE) {op} TRY_CAST({sql_str(val)} AS DATE)")
        elif t in (TIMESTAMP_TYPES | TIME_TYPES):
            clauses.append(f"TRY_CAST({c} AS TIMESTAMP) {op} TRY_CAST({sql_str(val)} AS TIMESTAMP)")
        elif t in BOOL_TYPES:
            v = str(val).strip().lower()
            if v in {"true", "t", "1", "yes", "y"}:
                clauses.append(f"CAST({c} AS BOOLEAN) {op} TRUE")
            elif v in {"false", "f", "0", "no", "n"}:
                clauses.append(f"CAST({c} AS BOOLEAN) {op} FALSE")
            else:
                clauses.append(f"CAST({c} AS VARCHAR) {op} {sql_str(val)}")
        else:
            clauses.append(f"{c} {op} {sql_str(val)}")

    return " AND ".join(clauses)