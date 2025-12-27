import io
import pandas as pd


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def dataframe_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    if df.shape[0] > 1_048_576 or df.shape[1] > 16_384:
        raise ValueError("Result too large for Excel limits. Export as CSV instead.")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="pivot")
    buf.seek(0)
    return buf.read()
