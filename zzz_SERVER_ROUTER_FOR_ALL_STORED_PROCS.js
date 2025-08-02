from fastapi import APIRouter, Request
import pyodbc
from SERVER_DB_CONNECTION import SERVER_DB_CONNECTION_GET

ROUTER_FOR_ALL_STORED_PROCS = APIRouter()

@ROUTER_FOR_ALL_STORED_PROCS.post("/CALL_SP")
async def CALL_SP_HANDLER(request: Request):
    try:
        BODY = await request.json()
        SP_NAME = BODY.get("SP_NAME")
        PARAMS = BODY.get("PARAMS", {})

        CONN = DB_CONNECTION_GET()
        CURSOR = CONN.cursor()

        PARAM_PLACEHOLDERS = ', '.join([f"@{k}=?" for k in PARAMS])
        SQL = f"EXEC {SP_NAME} {PARAM_PLACEHOLDERS}" if PARAMS else f"EXEC {SP_NAME}"

        CURSOR.execute(SQL, *PARAMS.values())

        if CURSOR.description:
            COLUMNS = [column[0] for column in CURSOR.description]
            ROWS = [dict(zip(COLUMNS, row)) for row in CURSOR.fetchall()]
            RESULT = ROWS
        else:
            RESULT = []

        CURSOR.close()
        CONN.close()

        return {"SP_NAME": SP_NAME, "RESULT": RESULT}

    except Exception as E:
        return {"error": str(E)}

