import os
import streamlit as st
from typing import Optional, Tuple

from .models import DataSource


def sidebar_source_and_settings() -> Tuple[Optional[DataSource], int, int]:
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

    return src, int(preview_limit), int(max_pivot_cols)