from fastapi import FastAPI, APIRouter, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import pyodbc
import sys
sys.stdout.reconfigure(encoding='utf-8')

SP_RESULT_SET_TYPE = {}

def SERVER_DB_CONNECTION_GET():
    CONNECTION_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=104.40.11.248,3341;"
        "DATABASE=VIOLIN;"
        "UID=violin;"
        "PWD=Test123!"
    )
    return pyodbc.connect(CONNECTION_STRING, autocommit=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        conn = SERVER_DB_CONNECTION_GET()
        cursor = conn.cursor()
        cursor.execute("EXEC P_BACKEND_SP_METADATA")

        while True:
            rows = cursor.fetchall()
            if rows:
                break
            if not cursor.nextset():
                break

        for row in rows:
            SP_RESULT_SET_TYPE[row.SP_NAME] = row.RESULT_SET_TYPE.upper()

        cursor.close()
        conn.close()
        print(f"✅ Loaded metadata for {len(SP_RESULT_SET_TYPE)} stored procedures.")
    except Exception as e:
        print(f"❌ Error loading stored procedure metadata: {str(e)}")

    yield
    print("🛑 Shutting down backend app...")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

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

@router.post("/CLIENT_LOG")
async def CLIENT_LOG(entries: dict = Body(...)):
    try:
        for e in entries.get("entries", []):
            print(f"[PHONE] {e.get('t')} {e.get('level')} {e.get('tag')} :: {e.get('msg')} {e.get('extra')}")
    except Exception as ex:
        print(f"[PHONE] log parse error: {ex}")
    return {"ok": True}

@app.get("/")
async def root():
    return {"message": "🎻 VIOLIN_MVP backend server is running."}

app.include_router(router)
