import streamlit as st
import pandas as pd
import duckdb

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
                if pivot_mode == "Long":
                    # Long pivot
                    dims_sql = ", ".join(row_dims)
                    if agg_func == "COUNT":
                        sql = f"SELECT {dims_sql}, COUNT(*) as value FROM data GROUP BY {dims_sql}"
                    else:
                        sql = f"SELECT {dims_sql}, {agg_func}({measure}) as value FROM data GROUP BY {dims_sql}"
                else:
                    # Wide pivot
                    # First, check distinct count of column dimension
                    distinct_count = con.execute(f"SELECT COUNT(DISTINCT {col_dim}) FROM data").fetchone()[0]
                    if distinct_count > 50:
                        st.error(f"Column dimension has {distinct_count} distinct values. Wide pivot is limited to 50.")
                        st.stop()

                    # Get distinct values
                    distinct_vals = con.execute(f"SELECT DISTINCT {col_dim} FROM data ORDER BY 1").df()[
                        col_dim].tolist()

                    # Build CASE statements
                    case_statements = []
                    for val in distinct_vals:
                        if agg_func == "COUNT":
                            case = f"SUM(CASE WHEN {col_dim} = '{val}' THEN 1 ELSE 0 END) AS \"{val}\""
                        else:
                            case = f"{agg_func}(CASE WHEN {col_dim} = '{val}' THEN {measure} END) AS \"{val}\""
                        case_statements.append(case)

                    dims_sql = ", ".join(row_dims)
                    select_list = f"{dims_sql}, " + ", ".join(case_statements)

                    sql = f"SELECT {select_list} FROM data GROUP BY {dims_sql}"

                result = con.execute(sql).df()
                st.dataframe(result)

    with st.expander("Preview Data"):
        st.dataframe(df.head(20))