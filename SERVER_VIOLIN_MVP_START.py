#### .\.venv\Scripts\activate #####
#### .\venv\Scripts\activate #####
#### uvicorn SERVER_VIOLIN_MVP_START:app --host 0.0.0.0 --port 8000 --reload #####
#### to verify: http://localhost:8000 #####
#### to verify: 
""" curl -X POST "http://localhost:8000/CALL_SP" ^
  -H "Content-Type: application/json" ^
  -d "{\"SP_NAME\":\"P_CLIENT_VIOLINIST_INS\",\"PARAMS\":{\"DEVICE_ID\":\"abc\",\"IP_ADDRESS\":\"127.0.0.1\",\"LATITUDE\":10.77,\"LONGITUDE\":106.69}}"
 """
#### curl -X POST "http://192.168.1.131:8000'/CALL_SP" -H "Content-Type: application/json" -d "{\"SP_NAME\": \"P_CLIENT_DD_SONG\", \"PARAMS\": {\"VIOLINIST_ID\": 123, \"FILTER_TEXT\": \"bach\"}}"


#### pip freeze > SERVER_VIOLIN_MVP_requirements.txt ####

#### dos prompt #2: ipconfig...get IPv4 Address and paste into CLIENT_STEP_1_REGISTER.js #####
#### npx expo start --clear ##### 
#### Step 1: Expo starts the app from index.js (or index.ts) #####
#### Step 2: App loads app/_layout.tsx (or app/_layout.jsx) #####
#### Step 3: Initial screen loads from app/(tabs)/index.tsx #####
#### github token: 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 #####

from fastapi import FastAPI, APIRouter, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import pyodbc

# === Global Metadata Cache ===
SP_RESULT_SET_TYPE = {}  # Example: { 'P_CLIENT_SONG_INS': 'SINGLE_RECORD' }

# === DB Connection ===
def SERVER_DB_CONNECTION_GET():
    CONNECTION_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=104.40.11.248,3341;"
        "DATABASE=VIOLIN;"
        "UID=violin;"
        "PWD=Test123!"
    )
    return pyodbc.connect(CONNECTION_STRING, autocommit=True)

# === Lifespan Startup/Shutdown Handler ===
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

    # Optional cleanup logic here
    print("🛑 Shutting down backend app...")

# === Initialize FastAPI App ===
app = FastAPI(lifespan=lifespan)

# === CORS (Allow All Origins) ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with exact domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Dynamic Stored Procedure Router ===
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

# === Root Endpoint ===
@app.get("/")
async def root():
    return {"message": "🎻 VIOLIN_MVP backend server is running."}

# === Register API Routes ===
app.include_router(router)
