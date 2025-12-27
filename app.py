import streamlit as st
import pandas as pd

st.set_page_config(page_title="CSV Pivot App", layout="wide")
st.title("CSV Pivot App")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write(f"File: {uploaded_file.name}")
    st.write(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")

    with st.expander("Preview Data"):
        st.dataframe(df.head(20))