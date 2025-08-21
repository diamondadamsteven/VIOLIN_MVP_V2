# SERVER_VIOLIN_MVP_START.py
from fastapi import FastAPI, APIRouter, Request, Body, Path
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Dict, List, Any, Iterable, Tuple, Optional, Union
import pyodbc
import sys
import json

sys.stdout.reconfigure(encoding='utf-8')

SP_RESULT_SET_TYPE: Dict[str, str] = {}
TABLE_COLUMNS: Dict[str, List[str]] = {}  # table -> allowed columns (from P_BACKEND_TABLE_INSERTS_METADATA)

# ─────────────────────────────────────────────────────────────
# DB connection
# ─────────────────────────────────────────────────────────────
def SERVER_DB_CONNECTION_GET():
    CONNECTION_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=104.40.11.248,3341;"
        "DATABASE=VIOLIN;"
        "UID=violin;"
        "PWD=Test123!"
    )
    # autocommit so single/batch inserts avoid extra txn overhead here
    return pyodbc.connect(CONNECTION_STRING, autocommit=True)

# ─────────────────────────────────────────────────────────────
# Startup: load SP + table/column metadata
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        conn = SERVER_DB_CONNECTION_GET()
        cursor = conn.cursor()

        # 1) Stored-proc metadata (existing)
        try:
            cursor.execute("EXEC P_BACKEND_SP_METADATA")
            while True:
                rows = cursor.fetchall()
                if rows:
                    for row in rows:
                        SP_RESULT_SET_TYPE[row.SP_NAME] = str(row.RESULT_SET_TYPE).upper()
                    break
                if not cursor.nextset():
                    break
        except Exception as e:
            print(f"❌ Error loading SP metadata: {e}")

        # 2) Table/column whitelist for safe inserts (NEW)
        try:
            # Returns TABLE_NAME, COLUMN_NAME
            cursor.execute("EXEC P_BACKEND_TABLE_INSERTS_METADATA")
            rows = cursor.fetchall()
            table_cols: Dict[str, List[str]] = {}
            for r in rows:
                t = str(r.TABLE_NAME).strip()
                c = str(r.COLUMN_NAME).strip()
                table_cols.setdefault(t, []).append(c)
            # Normalize: dedupe + stable order
            for t, cols in table_cols.items():
                seen = set()
                ordered = []
                for c in cols:
                    if c not in seen:
                        seen.add(c)
                        ordered.append(c)
                TABLE_COLUMNS[t] = ordered
        except Exception as e:
            print(f"❌ Error loading table/column metadata: {e}")

        cursor.close()
        conn.close()
        print(f"✅ Loaded {len(SP_RESULT_SET_TYPE)} SP metadata entries.")
        print(f"✅ Loaded column whitelists for {len(TABLE_COLUMNS)} tables.")
    except Exception as e:
        print(f"❌ Startup error: {str(e)}")

    yield
    print("🛑 Shutting down backend app...")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

# ─────────────────────────────────────────────────────────────
# Helpers for safe, fast inserts
# ─────────────────────────────────────────────────────────────
def _filter_row_for_table(table: str, row: Dict[str, Any]) -> Dict[str, Any]:
    cols = TABLE_COLUMNS.get(table)
    if not cols:
        raise ValueError(f"Table '{table}' not registered in metadata.")
    # Keep only whitelisted columns
    return {k: row[k] for k in row.keys() if k in cols}

def _ensure_list(payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    return []

def _insert_rows(table: str, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    # Filter by whitelist and drop empties
    filtered: List[Dict[str, Any]] = []
    for r in rows:
        fr = _filter_row_for_table(table, r or {})
        if fr:
            filtered.append(fr)
    if not filtered:
        return 0

    conn = SERVER_DB_CONNECTION_GET()
    try:
        cur = conn.cursor()

        # Group rows by exact column set (sorted) so placeholders align
        groups: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
        for r in filtered:
            key = tuple(sorted(r.keys()))
            groups.setdefault(key, []).append(r)

        inserted = 0
        for key_cols in groups:
            col_list = list(key_cols)
            cols_sql = ", ".join(col_list)
            placeholders = ", ".join(["?"] * len(col_list))
            sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

            vals = [tuple(r[c] for c in col_list) for r in groups[key_cols]]
            cur.fast_executemany = True  # important for multi-row insert speed
            cur.executemany(sql, vals)
            inserted += len(vals)

        cur.close()
        return inserted
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────
# Existing SP caller
# ─────────────────────────────────────────────────────────────
@router.post("/CALL_SP")
async def CALL_SP_HANDLER(request: Request):
    try:
        body = await request.json()
        sp_name = body.get("SP_NAME")
        params = body.get("PARAMS", {})

        if sp_name not in SP_RESULT_SET_TYPE:
            return {"error": f"Stored procedure '{sp_name}' not found in metadata cache."}

        conn = SERVER_DB_CONNECTION_GET()
        cursor = conn.cursor()

        placeholders = ', '.join([f"@{k}=?" for k in params])
        sql = f"EXEC {sp_name} {placeholders}" if params else f"EXEC {sp_name}"
        cursor.execute(sql, *params.values())

        result_type = SP_RESULT_SET_TYPE[sp_name]
        result = []

        if result_type == "NONE" or cursor.description is None:
            result = []
        else:
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            result = rows[0] if result_type == "SINGLE_RECORD" and rows else rows

        cursor.close()
        conn.close()

        return {"SP_NAME": sp_name, "RESULT": result}

    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────────────────────
# Phone console mirror (unchanged)
# ─────────────────────────────────────────────────────────────
@router.post("/CLIENT_LOG")
async def client_log(request: Request):
    data = await request.json()
    entries = data.get("LOG_ENTRY", [])

    conn = SERVER_DB_CONNECTION_GET()
    cursor = conn.cursor()

    sql = """
    INSERT INTO CLIENT_DB_APP_LOG (
      MOBILE_DEVICE_ID,
      MOBILE_DEVICE_PLATFORM,
      DT_LOG_ENTRY,
      REACT_FILE_NAME,
      REACT_FUNCTION_NAME,
      REACT_STEP_NAME,
      START_END_ERROR_OR_STEP,
      LOG_MSG,
      CLIENT_APP_VARIABLES_JSON,
      CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME_JSON,
      CLIENT_DB_LOG_WEBSOCKET_CONNECTION_JSON,
      CLIENT_DB_LOG_WEBSOCKET_MESSAGE_JSON,
      LOCAL_VARIABLES_JSON
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def to_dt(s):
      # Accept ISO strings; store as DATETIME2
      try: return datetime.fromisoformat(s.replace('Z', '+00:00'))
      except: return None

    for e in entries:
        cursor.execute(sql,
            e.get("MOBILE_DEVICE_ID"),
            e.get("MOBILE_DEVICE_PLATFORM"),
            to_dt(e.get("DT_LOG_ENTRY")),
            e.get("REACT_FILE_NAME"),
            e.get("REACT_FUNCTION_NAME"),
            e.get("REACT_STEP_NAME"),
            e.get("START_END_ERROR_OR_STEP"),
            e.get("LOG_MSG"),
            json.dumps(e.get("CLIENT_APP_VARIABLES_JSON")),
            json.dumps(e.get("CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME_JSON")),
            json.dumps(e.get("CLIENT_DB_LOG_WEBSOCKET_CONNECTION_JSON")),
            json.dumps(e.get("CLIENT_DB_LOG_WEBSOCKET_MESSAGE_JSON")),
            json.dumps(e.get("LOCAL_VARIABLES_JSON")),
        )

    conn.commit()
    return {"status": "ok", "count": len(entries)}

# ─────────────────────────────────────────────────────────────
# NEW: Generic insert endpoint
# Accepts a single object or an array of rows.
# Only inserts whitelisted columns for tables registered by P_BACKEND_TABLE_INSERTS_METADATA.
# Example: POST /INSERT_TABLE/CLIENT_DB_LOG_WEBSOCKET_MESSAGE
# ─────────────────────────────────────────────────────────────
@router.post("/INSERT_TABLE/{table}")
async def INSERT_TABLE(
    table: str = Path(..., description="Target table name (must be in metadata)"),
    rows: Union[Dict[str, Any], List[Dict[str, Any]]] = Body(...),
):
    try:
        if table not in TABLE_COLUMNS:
            return {"ok": False, "error": f"Table '{table}' not registered in metadata."}
        data = _ensure_list(rows)
        n = _insert_rows(table, data)
        return {"ok": True, "rows_inserted": n}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ─────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"ok": True, "mode": "backend", "version": 1}

@app.get("/")
async def root():
    return {"message": "🎻 VIOLIN_MVP backend server is running."}

app.include_router(router)
