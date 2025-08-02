import pyodbc

def SERVER_DB_CONNECTION_GET():
    CONNECTION_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=104.40.11.248,3341;"
        "DATABASE=VIOLIN;"
        "UID=violin;"
        "PWD=Test123!"
    )
    return pyodbc.connect(CONNECTION_STRING)
