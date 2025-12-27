import re

SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_COLNAME_RE = re.compile(r"[^A-Za-z0-9_]+")


def q_ident(name: str) -> str:
    """Quote an identifier safely for DuckDB SQL."""
    if SAFE_IDENT_RE.match(name):
        return name
    return '"' + name.replace('"', '""') + '"'


def sql_str(s: str) -> str:
    """Safely quote a string value for SQL."""
    return "'" + str(s).replace("'", "''") + "'"


def normalize_duckdb_type(t: str) -> str:
    """
    Normalize column_type strings from DESCRIBE into a basic type token.
    Example: 'DECIMAL(18,2)' -> 'DECIMAL'
    """
    t = (t or "").upper().strip()
    if "(" in t:
        t = t.split("(", 1)[0].strip()
    return t
