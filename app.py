import streamlit as st
import pandas as pd
import duckdb
import re
import os
from dataclasses import dataclass
from typing import Optional


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
        st.session_state["duckdb_con"] = con
    return st.session_state["duckdb_con"]


def relation_for_source(src: DataSource) -> str:
    """Return a DuckDB relation expression for the data source."""
    if src.kind == "path":
        # DuckDB is happier with forward slashes on Windows
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


def run_long_pivot(con, src, row_dims, measure, agg_func):
    """Run a long (grouped) pivot."""
    rel = relation_for_source(src)
    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    if agg_func == "COUNT":
        sql = f"SELECT {dims_sql}, COUNT(*) as value FROM {rel} GROUP BY {dims_sql}"
    else:
        sql = f"SELECT {dims_sql}, {agg_func}({q_ident(measure)}) as value FROM {rel} GROUP BY {dims_sql}"
    return con.execute(sql).df()


def run_wide_pivot(con, src, row_dims, col_dim, measure, agg_func, max_cols=50):
    """Run a wide pivot."""
    rel = relation_for_source(src)

    # Check distinct count
    distinct_count = con.execute(f"SELECT COUNT(DISTINCT {q_ident(col_dim)}) FROM {rel}").fetchone()[0]
    if distinct_count > max_cols:
        raise ValueError(f"Column dimension has {distinct_count} distinct values. Wide pivot is limited to {max_cols}.")

    # Get distinct values
    distinct_vals = con.execute(
        f"SELECT DISTINCT {q_ident(col_dim)} AS v FROM {rel} ORDER BY 1"
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

    sql = f"SELECT {select_list} FROM {rel} GROUP BY {dims_sql}"
    return con.execute(sql).df()


# ----------------------------
# Streamlit UI
# ----------------------------

st.set_page_config(page_title="CSV Pivot App", layout="wide")
st.title("CSV Pivot App")

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

if src is not None:
    con = ensure_con()

    # Get column information
    columns_df = get_columns(con, src)
    all_columns = columns_df['column_name'].tolist()

    col1, col2 = st.columns(2)

    with col1:
        st.write("Columns:")
        st.dataframe(columns_df)

    with col2:
        st.subheader("Pivot Configuration")

        pivot_mode = st.radio("Pivot Mode", ["Long", "Wide"], horizontal=True)

        row_dims = st.multiselect("Row Dimensions", all_columns)
        measure = st.selectbox("Measure Column", all_columns)
        agg_func = st.selectbox("Aggregation", ["SUM", "COUNT", "AVG", "MIN", "MAX"])

        if pivot_mode == "Wide":
            col_dim = st.selectbox("Column Dimension (for wide pivot)", all_columns)
        else:
            col_dim = None

        if st.button("Run Pivot"):
            if not row_dims:
                st.error("Please select at least one row dimension.")
            else:
                try:
                    if pivot_mode == "Long":
                        result = run_long_pivot(con, src, row_dims, measure, agg_func)
                    else:
                        result = run_wide_pivot(con, src, row_dims, col_dim, measure, agg_func, max_cols=50)
                    st.dataframe(result)
                except Exception as e:
                    st.error(f"Error: {e}")

    # Preview data
    with st.expander("Preview Data"):
        rel = relation_for_source(src)
        preview_df = con.execute(f"SELECT * FROM {rel} LIMIT 20").df()
        st.dataframe(preview_df)
else:
    st.info("Please load a CSV file using the sidebar.")