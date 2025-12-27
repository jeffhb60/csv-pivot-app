import streamlit as st
import pandas as pd
import duckdb
import re
import os
import io
import tempfile
import hashlib
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from pivot_app import DataSource, q_ident, sql_str, normalize_duckdb_type


# ----------------------------
# Data structures
# ----------------------------

# @dataclass
# class DataSource:
#     kind: str  # "path" or "upload"
#     path: Optional[str] = None
#     uploaded_bytes: Optional[bytes] = None
#     name: Optional[str] = None


# ----------------------------
# Helper functions
# ----------------------------

SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_COLNAME_RE = re.compile(r"[^A-Za-z0-9_]+")

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


# def q_ident(name: str) -> str:
#     """Quote an identifier safely for DuckDB SQL."""
#     if SAFE_IDENT_RE.match(name):
#         return name
#     return '"' + name.replace('"', '""') + '"'
#
#
# def sql_str(s: str) -> str:
#     """Safely quote a string value for SQL."""
#     return "'" + str(s).replace("'", "''") + "'"


def ensure_con() -> duckdb.DuckDBPyConnection:
    """Ensure DuckDB connection exists in session state."""
    if "duckdb_con" not in st.session_state:
        con = duckdb.connect(database=":memory:")
        # Let DuckDB use parallelism, but avoid hardcoding a single value
        threads = max(1, (os.cpu_count() or 4) // 2)
        con.execute(f"PRAGMA threads={threads};")
        st.session_state["duckdb_con"] = con
    return st.session_state["duckdb_con"]


def _upload_cache_key(name: str, b: bytes) -> str:
    h = hashlib.md5(b).hexdigest()  # stable enough for caching
    return f"upload_temp_path::{name}::{len(b)}::{h}"


def relation_for_source(src: DataSource) -> str:
    """
    Return a DuckDB relation expression for the data source.

    IMPORTANT: For uploads, write the temp file ONCE and reuse it
    to avoid leaking temp files and re-reading different files on reruns.
    """
    if src.kind == "path":
        path = (src.path or "").replace("\\", "/")
        return f"read_csv_auto({sql_str(path)})"

    if src.kind == "upload":
        if src.uploaded_bytes is None:
            raise ValueError("Upload source missing bytes")

        key = _upload_cache_key(src.name or "upload.csv", src.uploaded_bytes)
        cache: Dict[str, str] = st.session_state.setdefault("upload_temp_paths", {})

        if key not in cache or not os.path.exists(cache[key]):
            # Create a new temp file (once per unique upload)
            fd, temp_path = tempfile.mkstemp(suffix=".csv")
            os.close(fd)
            with open(temp_path, "wb") as f:
                f.write(src.uploaded_bytes)
            cache[key] = temp_path

        temp_path = cache[key].replace("\\", "/")
        return f"read_csv_auto({sql_str(temp_path)})"

    raise ValueError(f"Unknown source kind: {src.kind}")


def get_columns(con: duckdb.DuckDBPyConnection, src: DataSource) -> pd.DataFrame:
    """Get column information from the data source."""
    rel = relation_for_source(src)
    # DuckDB can't DESCRIBE a table-function call directly; wrap in SELECT
    return con.execute(f"DESCRIBE SELECT * FROM {rel}").df()


# def normalize_duckdb_type(t: str) -> str:
#     """
#     Normalize column_type strings from DESCRIBE into a basic type token.
#     Example: 'DECIMAL(18,2)' -> 'DECIMAL'
#     """
#     t = (t or "").upper().strip()
#     if "(" in t:
#         t = t.split("(", 1)[0].strip()
#     return t


def build_where(filters: List[Dict[str, Any]], col_types: Dict[str, str]) -> str:
    """
    Build a WHERE clause from a list of filters.

    Each filter dict: {"col": str, "op": str, "value": str}
    """
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

        # null checks
        if op == "is_null":
            clauses.append(f"{c} IS NULL")
            continue
        if op == "not_null":
            clauses.append(f"{c} IS NOT NULL")
            continue

        # string-ish ops (cast to VARCHAR so they work on non-text columns too)
        if op in {"contains", "startswith", "endswith"}:
            c_txt = f"CAST({c} AS VARCHAR)"
            if op == "contains":
                clauses.append(f"{c_txt} ILIKE '%' || {sql_str(val)} || '%'")
            elif op == "startswith":
                clauses.append(f"{c_txt} ILIKE {sql_str(str(val) + '%')}")
            else:  # endswith
                clauses.append(f"{c_txt} ILIKE {sql_str('%' + str(val))}")
            continue

        # typed comparisons for =, !=, >, >=, <, <=
        if t in NUMERIC_TYPES:
            clauses.append(
                f"TRY_CAST({c} AS DOUBLE) {op} TRY_CAST({sql_str(val)} AS DOUBLE)"
            )
        elif t in DATE_TYPES:
            clauses.append(
                f"TRY_CAST({c} AS DATE) {op} TRY_CAST({sql_str(val)} AS DATE)"
            )
        elif t in (TIMESTAMP_TYPES | TIME_TYPES):
            clauses.append(
                f"TRY_CAST({c} AS TIMESTAMP) {op} TRY_CAST({sql_str(val)} AS TIMESTAMP)"
            )
        elif t in BOOL_TYPES:
            # Accept common boolean inputs
            v = str(val).strip().lower()
            if v in {"true", "t", "1", "yes", "y"}:
                clauses.append(f"CAST({c} AS BOOLEAN) {op} TRUE")
            elif v in {"false", "f", "0", "no", "n"}:
                clauses.append(f"CAST({c} AS BOOLEAN) {op} FALSE")
            else:
                # fall back to string compare if user enters something weird
                clauses.append(f"CAST({c} AS VARCHAR) {op} {sql_str(val)}")
        else:
            # default: string compare
            clauses.append(f"{c} {op} {sql_str(val)}")

    return " AND ".join(clauses)


def _safe_wide_colname(val: Any, used: set) -> str:
    """
    Make a stable, Excel-friendly, SQL-safe column alias for wide pivots.
    Also avoids collisions by appending a short hash.
    """
    s = str(val)
    base = SAFE_COLNAME_RE.sub("_", s).strip("_")
    if not base:
        base = "col"
    base = base[:40]  # keep it reasonable
    h = hashlib.md5(s.encode("utf-8")).hexdigest()[:6]
    name = f"{base}_{h}"
    # ensure uniqueness even if base+hash collides (rare, but let's be adults)
    i = 2
    while name in used:
        name = f"{base}_{h}_{i}"
        i += 1
    used.add(name)
    return name


def run_long_pivot(
    con: duckdb.DuckDBPyConnection,
    src: DataSource,
    row_dims: List[str],
    measure: str,
    agg_func: str,
    where_sql: str = "",
    limit: int = 2000,
) -> pd.DataFrame:
    """Run a long (grouped) pivot (deterministic preview)."""
    rel = relation_for_source(src)
    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    if agg_func == "COUNT":
        sql = f"""
        SELECT {dims_sql}, COUNT(*) AS value
        FROM {rel}
        {where_clause}
        GROUP BY {dims_sql}
        ORDER BY value DESC
        LIMIT {int(limit)}
        """
    else:
        sql = f"""
        SELECT {dims_sql}, {agg_func}({q_ident(measure)}) AS value
        FROM {rel}
        {where_clause}
        GROUP BY {dims_sql}
        ORDER BY value DESC
        LIMIT {int(limit)}
        """
    return con.execute(sql).df()


def run_wide_pivot(
    con: duckdb.DuckDBPyConnection,
    src: DataSource,
    row_dims: List[str],
    col_dim: str,
    measure: str,
    agg_func: str,
    where_sql: str = "",
    max_cols: int = 200,
    limit: int = 2000,
) -> pd.DataFrame:
    """Run a wide pivot (with safer column names + deterministic preview)."""
    rel = relation_for_source(src)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    distinct_count = con.execute(
        f"SELECT COUNT(DISTINCT {q_ident(col_dim)}) FROM {rel} {where_clause}"
    ).fetchone()[0]
    if distinct_count > max_cols:
        raise ValueError(
            f"Column dimension has {distinct_count} distinct values. "
            f"Wide pivot is limited to {max_cols}."
        )

    distinct_vals = con.execute(
        f"SELECT DISTINCT {q_ident(col_dim)} AS v FROM {rel} {where_clause} ORDER BY 1"
    ).df()["v"].tolist()

    used_aliases = set()
    case_statements = []
    for val in distinct_vals:
        alias = _safe_wide_colname(val, used_aliases)
        if agg_func == "COUNT":
            case = (
                f"SUM(CASE WHEN {q_ident(col_dim)} = {sql_str(val)} THEN 1 ELSE 0 END) "
                f"AS {q_ident(alias)}"
            )
        else:
            case = (
                f"{agg_func}(CASE WHEN {q_ident(col_dim)} = {sql_str(val)} "
                f"THEN {q_ident(measure)} END) AS {q_ident(alias)}"
            )
        case_statements.append(case)

    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    select_list = f"{dims_sql}, " + ", ".join(case_statements)

    # Deterministic ordering by row dimensions (best-effort for preview)
    order_sql = dims_sql if dims_sql else "1"

    sql = f"""
    SELECT {select_list}
    FROM {rel}
    {where_clause}
    GROUP BY {dims_sql}
    ORDER BY {order_sql}
    LIMIT {int(limit)}
    """
    return con.execute(sql).df()


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to CSV bytes."""
    return df.to_csv(index=False).encode("utf-8")


def dataframe_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Excel bytes."""
    if df.shape[0] > 1_048_576 or df.shape[1] > 16_384:
        raise ValueError("Result too large for Excel limits. Export as CSV instead.")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="pivot")
    buf.seek(0)
    return buf.read()


# ----------------------------
# Streamlit UI
# ----------------------------

st.set_page_config(page_title="CSV Pivot App", layout="wide")
st.title("CSV Pivot App")

# Store filters as a LIST so multiple filters can apply to the same column
if "filters" not in st.session_state:
    st.session_state.filters = []  # list[{"col":..., "op":..., "value":...}]

with st.sidebar:
    st.header("Data Source")
    source_type = st.radio("Source Type", ["Upload File", "Local File Path"])

    src: Optional[DataSource] = None
    if source_type == "Upload File":
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        if uploaded_file is not None:
            src = DataSource(kind="upload", uploaded_bytes=uploaded_file.getvalue(), name=uploaded_file.name)
    else:
        file_path = st.text_input("File Path", value="")
        if file_path and os.path.exists(file_path):
            src = DataSource(kind="path", path=file_path)

    st.divider()
    st.header("Settings")
    preview_limit = st.number_input("Preview Row Limit", min_value=50, max_value=10000, value=2000, step=50)
    max_pivot_cols = st.number_input("Max Wide Pivot Columns", min_value=10, max_value=1000, value=200, step=10)

if src is None:
    st.info("Please load a CSV file using the sidebar.")
    st.stop()

con = ensure_con()

columns_df = get_columns(con, src)
all_columns = columns_df["column_name"].tolist()

# Build type map for typed filters
col_types = dict(zip(columns_df["column_name"], columns_df["column_type"]))

tab1, tab2, tab3, tab4 = st.tabs(["Data", "Pivot", "Filters", "Export"])

with tab1:
    st.write("Columns:")
    st.dataframe(columns_df, use_container_width=True)

    with st.expander("Preview Data"):
        rel = relation_for_source(src)
        preview_df = con.execute(f"SELECT * FROM {rel} LIMIT 20").df()
        st.dataframe(preview_df, use_container_width=True)

with tab2:
    st.subheader("Pivot Configuration")

    pivot_mode = st.radio("Pivot Mode", ["Long", "Wide"], horizontal=True)

    row_dims = st.multiselect("Row Dimensions", all_columns)
    measure = st.selectbox("Measure Column", all_columns)
    agg_func = st.selectbox("Aggregation", ["SUM", "COUNT", "AVG", "MIN", "MAX"])

    col_dim = None
    if pivot_mode == "Wide":
        col_dim = st.selectbox("Column Dimension (for wide pivot)", all_columns)

    if st.button("Run Pivot", type="primary"):
        if not row_dims:
            st.error("Please select at least one row dimension.")
        else:
            try:
                where_sql = build_where(st.session_state.filters, col_types)
                if pivot_mode == "Long":
                    result = run_long_pivot(
                        con, src, row_dims, measure, agg_func, where_sql, limit=int(preview_limit)
                    )
                else:
                    if not col_dim:
                        st.error("Please select a column dimension for wide pivot.")
                        st.stop()
                    result = run_wide_pivot(
                        con, src, row_dims, col_dim, measure, agg_func,
                        where_sql, max_cols=int(max_pivot_cols), limit=int(preview_limit)
                    )

                st.session_state["last_result"] = result
                st.session_state["last_pivot_mode"] = pivot_mode
                st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")

with tab3:
    st.subheader("Filters")

    c1, c2, c3, c4 = st.columns([3, 2, 3, 2])
    with c1:
        filter_column = st.selectbox("Column", [""] + all_columns, key="filter_col")
    with c2:
        filter_op = st.selectbox(
            "Operator",
            ["=", "!=", ">", ">=", "<", "<=", "contains", "startswith", "endswith", "is_null", "not_null"],
            key="filter_op",
        )
    with c3:
        filter_value = st.text_input("Value", key="filter_val")
    with c4:
        add = st.button("Add Filter")

    if add:
        if filter_column:
            st.session_state.filters.append({"col": filter_column, "op": filter_op, "value": filter_value})

    if st.session_state.filters:
        st.write("Active Filters:")
        # Show filters with a remove button per row
        for i, f in enumerate(list(st.session_state.filters)):
            col = f.get("col", "")
            op = f.get("op", "")
            val = f.get("value", "")
            row = st.columns([8, 2])
            row[0].write(f"- {col} {op} {val}")
            if row[1].button("Remove", key=f"rm_filter_{i}"):
                st.session_state.filters.pop(i)
                st.rerun()

        if st.button("Clear All Filters"):
            st.session_state.filters = []

with tab4:
    st.subheader("Export")

    if "last_result" not in st.session_state:
        st.info("Run a pivot first to export results.")
    else:
        df = st.session_state["last_result"]
        st.write(f"Result shape: {df.shape[0]} rows, {df.shape[1]} columns")

        csv_bytes = dataframe_to_csv_bytes(df)
        st.download_button(
            label="Download as CSV",
            data=csv_bytes,
            file_name="pivot_result.csv",
            mime="text/csv",
        )

        try:
            xlsx_bytes = dataframe_to_xlsx_bytes(df)
            st.download_button(
                label="Download as Excel",
                data=xlsx_bytes,
                file_name="pivot_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except ValueError as e:
            st.warning(f"Cannot export to Excel: {e}")
