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

        row_dims = st.multiselect("Row Dimensions", all_columns)
        measure = st.selectbox("Measure Column", all_columns)
        agg_func = st.selectbox("Aggregation", ["SUM", "COUNT", "AVG", "MIN", "MAX"])

        if st.button("Run Pivot"):
            if not row_dims:
                st.error("Please select at least one row dimension.")
            else:
                # Build SQL
                dims_sql = ", ".join(row_dims)
                if agg_func == "COUNT":
                    sql = f"SELECT {dims_sql}, COUNT(*) as value FROM data GROUP BY {dims_sql}"
                else:
                    sql = f"SELECT {dims_sql}, {agg_func}({measure}) as value FROM data GROUP BY {dims_sql}"

                result = con.execute(sql).df()
                st.dataframe(result)

    with st.expander("Preview Data"):
        st.dataframe(df.head(20))