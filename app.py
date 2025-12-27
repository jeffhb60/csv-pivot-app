import streamlit as st
import pandas as pd
import duckdb
import re
import os
import io
import tempfile
from dataclasses import dataclass
from typing import Optional, List, Dict


# ----------------------------
# Data structures
# ----------------------------

@dataclass
class DataSource:
    kind: str  # "path" or "upload"
    path: Optional[str] = None
    uploaded_bytes: Optional[bytes] = None
    name: Optional[str] = None


# ----------------------------
# Helper functions
# ----------------------------

SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def q_ident(name: str) -> str:
    """Quote an identifier safely for DuckDB SQL."""
    if SAFE_IDENT_RE.match(name):
        return name
    return '"' + name.replace('"', '""') + '"'


def sql_str(s: str) -> str:
    """Safely quote a string value for SQL."""
    return "'" + s.replace("'", "''") + "'"


def ensure_con() -> duckdb.DuckDBPyConnection:
    """Ensure DuckDB connection exists in session state."""
    if "duckdb_con" not in st.session_state:
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA threads=4;")
        st.session_state["duckdb_con"] = con
    return st.session_state["duckdb_con"]


def relation_for_source(src: DataSource) -> str:
    """Return a DuckDB relation expression for the data source."""
    if src.kind == "path":
        path = src.path.replace("\\", "/")
        return f"read_csv_auto({sql_str(path)})"
    elif src.kind == "upload":
        import tempfile
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
            f.write(src.uploaded_bytes)
            temp_path = f.name.replace("\\", "/")
        return f"read_csv_auto({sql_str(temp_path)})"
    else:
        raise ValueError(f"Unknown source kind: {src.kind}")


def get_columns(con: duckdb.DuckDBPyConnection, src: DataSource):
    """Get column information from the data source."""
    rel = relation_for_source(src)
    return con.execute(f"DESCRIBE SELECT * FROM {rel}").df()


def build_where(filters: Dict[str, Dict[str, str]]) -> str:
    """Build WHERE clause from filters."""
    clauses = []
    for col, spec in filters.items():
        op = spec.get("op", "=")
        val = spec.get("value", "")
        c = q_ident(col)

        if op == "is_null":
            clauses.append(f"{c} IS NULL")
        elif op == "not_null":
            clauses.append(f"{c} IS NOT NULL")
        elif op == "contains":
            clauses.append(f"{c} ILIKE '%' || {sql_str(val)} || '%'")
        elif op == "startswith":
            clauses.append(f"{c} ILIKE {sql_str(val + '%')}")
        elif op == "endswith":
            clauses.append(f"{c} ILIKE {sql_str('%' + val)}")
        else:
            clauses.append(f"{c} {op} {sql_str(val)}")
    return " AND ".join(clauses) if clauses else ""


def run_long_pivot(con, src, row_dims, measure, agg_func, where_sql="", limit=2000):
    """Run a long (grouped) pivot."""
    rel = relation_for_source(src)
    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    if agg_func == "COUNT":
        sql = f"SELECT {dims_sql}, COUNT(*) as value FROM {rel} {where_clause} GROUP BY {dims_sql}"
    else:
        sql = f"SELECT {dims_sql}, {agg_func}({q_ident(measure)}) as value FROM {rel} {where_clause} GROUP BY {dims_sql}"

    # Add limit for preview
    sql += f" LIMIT {limit}"
    return con.execute(sql).df()


def run_wide_pivot(con, src, row_dims, col_dim, measure, agg_func, where_sql="", max_cols=200, limit=2000):
    """Run a wide pivot."""
    rel = relation_for_source(src)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    # Check distinct count (with filter)
    distinct_count = con.execute(f"SELECT COUNT(DISTINCT {q_ident(col_dim)}) FROM {rel} {where_clause}").fetchone()[0]
    if distinct_count > max_cols:
        raise ValueError(f"Column dimension has {distinct_count} distinct values. Wide pivot is limited to {max_cols}.")

    # Get distinct values (with filter)
    distinct_vals = con.execute(
        f"SELECT DISTINCT {q_ident(col_dim)} AS v FROM {rel} {where_clause} ORDER BY 1"
    ).df()["v"].tolist()

    # Build CASE statements
    case_statements = []
    for val in distinct_vals:
        if agg_func == "COUNT":
            case = f"SUM(CASE WHEN {q_ident(col_dim)} = {sql_str(val)} THEN 1 ELSE 0 END) AS {q_ident(str(val))}"
        else:
            case = f"{agg_func}(CASE WHEN {q_ident(col_dim)} = {sql_str(val)} THEN {q_ident(measure)} END) AS {q_ident(str(val))}"
        case_statements.append(case)

    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    select_list = f"{dims_sql}, " + ", ".join(case_statements)

    sql = f"SELECT {select_list} FROM {rel} {where_clause} GROUP BY {dims_sql}"

    # Add limit for preview
    sql += f" LIMIT {limit}"
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

# Initialize session state for filters
if "filters" not in st.session_state:
    st.session_state.filters = {}

# Sidebar for data source selection
with st.sidebar:
    st.header("Data Source")
    source_type = st.radio("Source Type", ["Upload File", "Local File Path"])

    src = None
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

if src is not None:
    con = ensure_con()

    # Get column information
    columns_df = get_columns(con, src)
    all_columns = columns_df['column_name'].tolist()

    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Data", "Pivot", "Filters", "Export"])

    with tab1:
        st.write("Columns:")
        st.dataframe(columns_df)

        with st.expander("Preview Data"):
            rel = relation_for_source(src)
            preview_df = con.execute(f"SELECT * FROM {rel} LIMIT 20").df()
            st.dataframe(preview_df)

    with tab2:
        st.subheader("Pivot Configuration")

        pivot_mode = st.radio("Pivot Mode", ["Long", "Wide"], horizontal=True)

        row_dims = st.multiselect("Row Dimensions", all_columns)
        measure = st.selectbox("Measure Column", all_columns)
        agg_func = st.selectbox("Aggregation", ["SUM", "COUNT", "AVG", "MIN", "MAX"])

        if pivot_mode == "Wide":
            col_dim = st.selectbox("Column Dimension (for wide pivot)", all_columns)
        else:
            col_dim = None

        if st.button("Run Pivot", type="primary"):
            if not row_dims:
                st.error("Please select at least one row dimension.")
            else:
                try:
                    where_sql = build_where(st.session_state.filters)
                    if pivot_mode == "Long":
                        result = run_long_pivot(con, src, row_dims, measure, agg_func, where_sql, limit=preview_limit)
                    else:
                        result = run_wide_pivot(con, src, row_dims, col_dim, measure, agg_func, where_sql,
                                                max_cols=max_pivot_cols, limit=preview_limit)

                    st.session_state["last_result"] = result
                    st.session_state["last_pivot_mode"] = pivot_mode

                    st.dataframe(result)
                except Exception as e:
                    st.error(f"Error: {e}")

    with tab3:
        st.subheader("Filters")

        col1, col2, col3 = st.columns(3)
        with col1:
            filter_column = st.selectbox("Column", [""] + all_columns, key="filter_col")
        with col2:
            filter_op = st.selectbox("Operator",
                                     ["=", "!=", ">", ">=", "<", "<=", "contains", "startswith", "endswith", "is_null",
                                      "not_null"], key="filter_op")
        with col3:
            filter_value = st.text_input("Value", key="filter_val")

        if st.button("Add Filter"):
            if filter_column:
                st.session_state.filters[filter_column] = {"op": filter_op, "value": filter_value}

        if st.session_state.filters:
            st.write("Active Filters:")
            for col, spec in st.session_state.filters.items():
                st.write(f"- {col} {spec['op']} {spec.get('value', '')}")

            if st.button("Clear All Filters"):
                st.session_state.filters = {}

    with tab4:
        st.subheader("Export")

        if "last_result" in st.session_state:
            df = st.session_state["last_result"]

            st.write(f"Result shape: {df.shape[0]} rows, {df.shape[1]} columns")

            # CSV Export
            csv_bytes = dataframe_to_csv_bytes(df)
            st.download_button(
                label="Download as CSV",
                data=csv_bytes,
                file_name="pivot_result.csv",
                mime="text/csv"
            )

            # Excel Export
            try:
                xlsx_bytes = dataframe_to_xlsx_bytes(df)
                st.download_button(
                    label="Download as Excel",
                    data=xlsx_bytes,
                    file_name="pivot_result.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except ValueError as e:
                st.warning(f"Cannot export to Excel: {e}")
        else:
            st.info("Run a pivot first to export results.")
else:
    st.info("Please load a CSV file using the sidebar.")