import os
import hashlib
import tempfile
import streamlit as st
import duckdb
from typing import Dict

from .models import DataSource
from .sql_utils import sql_str


def ensure_con() -> duckdb.DuckDBPyConnection:
    """Ensure DuckDB connection exists in session state."""
    if "duckdb_con" not in st.session_state:
        con = duckdb.connect(database=":memory:")
        threads = max(1, (os.cpu_count() or 4) // 2)
        con.execute(f"PRAGMA threads={threads};")
        st.session_state["duckdb_con"] = con
    return st.session_state["duckdb_con"]


def _upload_cache_key(name: str, b: bytes) -> str:
    h = hashlib.md5(b).hexdigest()
    return f"upload_temp_path::{name}::{len(b)}::{h}"


def relation_for_source(src: DataSource) -> str:
    """
    Return a DuckDB relation expression for the data source.
    Uploads are written ONCE per unique file and reused across reruns.
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
            fd, temp_path = tempfile.mkstemp(suffix=".csv")
            os.close(fd)
            with open(temp_path, "wb") as f:
                f.write(src.uploaded_bytes)
            cache[key] = temp_path

        temp_path = cache[key].replace("\\", "/")
        return f"read_csv_auto({sql_str(temp_path)})"

    raise ValueError(f"Unknown source kind: {src.kind}")


def get_columns(con: duckdb.DuckDBPyConnection, src: DataSource):
    """Get column information from the data source."""
    rel = relation_for_source(src)
    return con.execute(f"DESCRIBE SELECT * FROM {rel}").df()