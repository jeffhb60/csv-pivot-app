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
    st.write("Columns:")
    st.dataframe(columns_df)

    with st.expander("Preview Data"):
        st.dataframe(df.head(20))