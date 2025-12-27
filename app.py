import streamlit as st

from pivot_app.db import ensure_con, get_columns, relation_for_source
from pivot_app.filters import build_where
from pivot_app.pivot import run_long_pivot, run_wide_pivot
from pivot_app.export import dataframe_to_csv_bytes, dataframe_to_xlsx_bytes
from pivot_app.ui import sidebar_source_and_settings


st.set_page_config(page_title="CSV Pivot App", layout="wide")
st.title("CSV Pivot App")

# Filters stored as list for multi-filter support
if "filters" not in st.session_state:
    st.session_state.filters = []

src, preview_limit, max_pivot_cols = sidebar_source_and_settings()

if src is None:
    st.info("Please load a CSV file using the sidebar.")
    st.stop()

con = ensure_con()
columns_df = get_columns(con, src)
all_columns = columns_df["column_name"].tolist()
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
    col_dim = st.selectbox("Column Dimension (for wide pivot)", all_columns) if pivot_mode == "Wide" else None

    if st.button("Run Pivot", type="primary"):
        if not row_dims:
            st.error("Please select at least one row dimension.")
        else:
            try:
                where_sql = build_where(st.session_state.filters, col_types)
                if pivot_mode == "Long":
                    result = run_long_pivot(con, src, row_dims, measure, agg_func, where_sql, limit=preview_limit)
                else:
                    result = run_wide_pivot(
                        con, src, row_dims, col_dim, measure, agg_func,
                        where_sql, max_cols=max_pivot_cols, limit=preview_limit
                    )

                st.session_state["last_result"] = result
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

    if add and filter_column:
        st.session_state.filters.append({"col": filter_column, "op": filter_op, "value": filter_value})

    if st.session_state.filters:
        st.write("Active Filters:")
        for i, f in enumerate(list(st.session_state.filters)):
            row = st.columns([8, 2])
            row[0].write(f"- {f.get('col','')} {f.get('op','')} {f.get('value','')}")
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

        st.download_button(
            label="Download as CSV",
            data=dataframe_to_csv_bytes(df),
            file_name="pivot_result.csv",
            mime="text/csv",
        )

        try:
            st.download_button(
                label="Download as Excel",
                data=dataframe_to_xlsx_bytes(df),
                file_name="pivot_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except ValueError as e:
            st.warning(f"Cannot export to Excel: {e}")