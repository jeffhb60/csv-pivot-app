import hashlib
from typing import Any, List

import duckdb
import pandas as pd

from .db import relation_for_source
from .sql_utils import q_ident, sql_str, SAFE_COLNAME_RE


def _safe_wide_columns(val: Any, used: set) -> str:
    """Sanitize + hash wide pivot column aliases to avoid collisions/weird names."""
    s = str(val)
    base = SAFE_COLNAME_RE.sub("_", s).strip("_") or "col"
    base = base[:40]
    h = hashlib.md5(s.encode("utf-8")).hexdigest()[:6]
    name = f"{base}_{h}"
    i = 2
    while name in used:
        name = f"{base}_{h}_{i}"
        i += 1
    used.add(name)
    return name


def run_long_pivot(
    con: duckdb.DuckDBPyConnection,
    src,
    row_dims: List[str],
    measure: str,
    agg_func: str,
    where_sql: str = "",
    limit: int = 2000,
) -> pd.DataFrame:
    rel = relation_for_source(src)
    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    if agg_func == "COUNT":
        sql = f"""
        SELECT {dims_sql}, COUNT(*) AS value
        FROM {rel}
        {where_clause}
        GROUP BY {dims_sql}
        ORDER BY value DESC
        LIMIT {int(limit)}
        """
    else:
        sql = f"""
        SELECT {dims_sql}, {agg_func}({q_ident(measure)}) AS value
        FROM {rel}
        {where_clause}
        GROUP BY {dims_sql}
        ORDER BY value DESC
        LIMIT {int(limit)}
        """
    return con.execute(sql).df()


def run_wide_pivot(
    con: duckdb.DuckDBPyConnection,
    src,
    row_dims: List[str],
    col_dim: str,
    measure: str,
    agg_func: str,
    where_sql: str = "",
    max_cols: int = 200,
    limit: int = 2000,
) -> pd.DataFrame:
    rel = relation_for_source(src)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    distinct_count = con.execute(
        f"SELECT COUNT(DISTINCT {q_ident(col_dim)}) FROM {rel} {where_clause}"
    ).fetchone()[0]
    if distinct_count > max_cols:
        raise ValueError(f"Column dimension has {distinct_count} distinct values. Wide pivot is limited to {max_cols}.")

    distinct_vals = con.execute(
        f"SELECT DISTINCT {q_ident(col_dim)} AS v FROM {rel} {where_clause} ORDER BY 1"
    ).df()["v"].tolist()

    used = set()
    case_statements = []
    for val in distinct_vals:
        alias = _safe_wide_columns(val, used)
        if agg_func == "COUNT":
            case_statements.append(
                f"SUM(CASE WHEN {q_ident(col_dim)} = {sql_str(val)} THEN 1 ELSE 0 END) AS {q_ident(alias)}"
            )
        else:
            case_statements.append(
                f"{agg_func}(CASE WHEN {q_ident(col_dim)} = {sql_str(val)} THEN {q_ident(measure)} END) AS {q_ident(alias)}"
            )

    dims_sql = ", ".join(q_ident(d) for d in row_dims)
    select_list = f"{dims_sql}, " + ", ".join(case_statements)
    order_sql = dims_sql if dims_sql else "1"

    sql = f"""
    SELECT {select_list}
    FROM {rel}
    {where_clause}
    GROUP BY {dims_sql}
    ORDER BY {order_sql}
    LIMIT {int(limit)}
    """
    return con.execute(sql).df()