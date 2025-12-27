# CSV Pivot App (Streamlit + DuckDB)
***
A lightweight Streamlit app that builds **Excel-style pivot tables** from large CSV files using **DuckDB** under the hood.  
Supports both **Long (tidy)** and **Wide (spreadsheet)** pivot outputs, optional **filters**, and **CSV / Excel export**.

## 1. Features
***

- **Two data sources**
  - Upload a CSV file
  - Load a CSV from a local file path
- **Pivot modes**
  - **Long pivot** (grouped results with a single `value` column)
  - **Wide pivot** (distinct column values become output columns)
- **Filtering**
  - Add multiple filters (including multiple filters on the same column)
  - String operations: `contains`, `startswith`, `endswith`
  - Typed comparisons: numeric, date, timestamp, boolean (via `TRY_CAST`)
- **Safe wide pivot output**
  - Wide pivot column names are sanitized and made unique (hash suffix)
- **Export**
  - Download pivot results as **CSV**
  - Download pivot results as **Excel (.xlsx)** (with Excel row/col limit protection)
- **Performance-minded**
  - DuckDB query execution (better than loading huge CSVs fully into pandas)
  - Uploads are cached to a stable temp file to avoid repeated writes/leaks
  - Preview row limit to keep UI responsive

## 2. What "Long" vs "Wide" Means
***

### 2a. Long pivot (tidy / analytics-friendly)
Produces one row per group and one aggregated `value` column:

| region | year | value |
|-------|------|------:|
| East  | 2023 |   120 |
| East  | 2024 |   150 |

Great for filtering, modeling, charts, and large datasets.

### 2b. Wide pivot (Excel-style)
Turns distinct values of your column-dimension into columns:

| region | 2023 | 2024 |
|-------|-----:|-----:|
| East  |  120 |  150 |

Great for reporting and spreadsheets, but can explode in column count (so the app enforces a max)

## 3. Requirements
***

- Python 3.10+ recommended
- Works on Windows/macOS/Linux
- Dependencies:
  - `streamlit` version 1.52.2
  - `duckdb` version 1.4.3
  - `pandas` version 2.3.3
  - `openpyxl` version 3.1.5

## 4. Installation 
***

### 4a. Create and activate a virtual environment 
**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```
**macOS/Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
```
### 4b. Install dependencies 
If you have a `requirements.txt`
```bash
pip install -r requirements.txt
```

If you don't yet, install directly: 
```bash
pip install streamlit==1.52.2 duckdb==1.4.3 pandas==2.3.3 openpyxl==3.1.5
```
 - `streamlit` version 1.52.2
  - `duckdb` version 1.4.3
  - `pandas` version 2.3.3
  - `openpyxl` version 3.1.5

## 5. Running the App
***

```bash
streamlit run app.py
```
Streamlit will print a local URL (usually `http://localhost:8501`)

## 6. Using the App
***

### 6a. Load Data
In the sidebar:
* Choose Upload File and select a CSV <br/>or
* Choose Local File Path and paste the path to a CSV

### 6b. Configure Pivot
Go to the **Pivot** tab:
* Choose **Pivot Mode**: ``Long`` or ``Wide``
* Select **Row Dimensions** (group-by columns)
* Select **Measure Column**
* Select **Aggregation**: ``SUM``, ``COUNT``, ``AVG``, ``MIN``, ``MAX``
* If **Wide**, select **Column Dimension**

Click **Run Pivot**.

### 6c. Add Filters (Optional)
Go to the **Filters** tab:
* Pick a column, operator, and value
* Click **Add Filter**
* Remove filters individually or **Clear All**

Filters apply to both preview and pivot queries.

### 6d. Export Results
Go to the **Export** tab:
* Download as CSV
* Download as Excel (if within Excel row/col limits)

## 7. Notes of Filters and Types
Filters attempt to be type-aware using DuckDB’s inferred CSV schema:
* Numeric comparisons use ``TRY_CAST(... AS DOUBLE)``
* Dates use ``TRY_CAST(... AS DATE)``
* Timestamps use ``TRY_CAST(... AS TIMESTAMP)``
* Booleans accept: ``true/false``, ``t/f``, ``1/0``, ``yes/no``

If a cast fails, ``TRY_CAST`` returns ``NULL``, which may cause comparisons to evaluate to false (typical SQL behavior).

## 8. Project Structure
***

```markdown
csv-pivot-app/
  app.py
  pivot_app/
    __init__.py
    models.py
    sql_utils.py
    db.py
    filters.py
    pivots.py
    export.py
    ui.py
```

* ``models.py`` → DataSource
* ``db.py`` → DuckDB connection + CSV relation loading 
* ``filters.py`` → WHERE clause builder
* ``pivots.py`` → long/wide pivot queries
* ``export.py`` → CSV/Excel export helpers
* ``ui.py`` → sidebar helpers (optional)

## 9. Troubleshooting
***

### 9a. Wide pivot fails or is limited
Wide pivots create a column per distinct value in your column dimension.
If your column dimension has too many unique values, the app will block it based on **Max Wide Pivot Columns**.

Try:
* Switching to **Long**
* Filtering first to reduce distinct values
* Increasing the max column limit (careful: output may become huge)

### 9b. Excel Export Fails
***

Excel has hard limits: 
* Max Rows: **1,048,576**
* Max Columns: **16.384**

If your pivot exceeds those, export as CSV instead.  

## 10. Performance Tips
***
* Prefer Long Pivots for large datasets
* Use filters to reduce data size before pivoting
* Keep preview limits reasonable (2k - 5k is usually plenty)

## 11. Security Notes
This app is intended for local use.
Still, operators are whitelisted and inputs are sanitized to reduce accidental SQL injection risk when building queries.


