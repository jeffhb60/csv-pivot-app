import streamlit as st
import pandas as pd
import duckdb
import re

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


def run_long_pivot(con, table_name, row_dims, measure, agg_func):
    """Run a long (grouped) pivot."""
    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    if agg_func == "COUNT":
        sql = f"SELECT {dims_sql}, COUNT(*) as value FROM {table_name} GROUP BY {dims_sql}"
    else:
        sql = f"SELECT {dims_sql}, {agg_func}({q_ident(measure)}) as value FROM {table_name} GROUP BY {dims_sql}"
    return con.execute(sql).df()


def run_wide_pivot(con, table_name, row_dims, col_dim, measure, agg_func, max_cols=50):
    """Run a wide pivot."""
    # Check distinct count
    distinct_count = con.execute(f"SELECT COUNT(DISTINCT {q_ident(col_dim)}) FROM {table_name}").fetchone()[0]
    if distinct_count > max_cols:
        raise ValueError(f"Column dimension has {distinct_count} distinct values. Wide pivot is limited to {max_cols}.")

    # Get distinct values
    distinct_vals = con.execute(f"SELECT DISTINCT {q_ident(col_dim)} FROM {table_name} ORDER BY 1").df()[
        col_dim].tolist()

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

    sql = f"SELECT {select_list} FROM {table_name} GROUP BY {dims_sql}"
    return con.execute(sql).df()


# ----------------------------
# Streamlit UI
# ----------------------------

st.set_page_config(page_title="CSV Pivot App", layout="wide")
st.title("CSV Pivot App")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    # Load into DuckDB
    con = duckdb.connect(database=":memory:")
    df = pd.read_csv(uploaded_file)
    con.register("data", df)

    st.write(f"File: {uploaded_file.name}")
    st.write(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")

    # Get column information
    columns_df = con.execute("DESCRIBE data").df()
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
                        result = run_long_pivot(con, "data", row_dims, measure, agg_func)
                    else:
                        result = run_wide_pivot(con, "data", row_dims, col_dim, measure, agg_func, max_cols=50)
                    st.dataframe(result)
                except Exception as e:
                    st.error(f"Error: {e}")

    with st.expander("Preview Data"):
        st.dataframe(df.head(20))