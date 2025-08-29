#!/usr/bin/env python3
"""
SQLite Logging Utilities for VIOLIN_MVP_V2

This module provides SQLite-based logging functions that replace SQL Server logging
to improve audio processing performance. All logging operations write to local SQLite
database instead of network SQL Server.
"""

import sqlite3
import os
from datetime import datetime
from typing import Any, Dict, Optional, Union
from contextlib import contextmanager

# Database path
SQLITE_DB_PATH = r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db"

def get_sqlite_connection():
    """Get a connection to the SQLite logging database."""
    if not os.path.exists(SQLITE_DB_PATH):
        raise FileNotFoundError(f"SQLite database not found: {SQLITE_DB_PATH}. Run SERVER_ENGINE_CREATE_SQLITE_DB_AND_TABLES.py first.")
    
    return sqlite3.connect(SQLITE_DB_PATH)

@contextmanager
def sqlite_connection():
    """Context manager for SQLite connections."""
    conn = get_sqlite_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def truncate_all_logging_tables() -> None:
    """
    Truncate all ENGINE_DB_LOG_* tables in SQLite.
    Use this function to clear all logging data on server startup.
    """
    tables = [
        "ENGINE_DB_LOG_FUNCTIONS",
        "ENGINE_DB_LOG_FUNCTION_ERROR", 
        "ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME",
        "ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME",
        "ENGINE_DB_LOG_RECORDING_CONFIG",
        "ENGINE_DB_LOG_WEBSOCKET_CONNECTION",
        "ENGINE_DB_LOG_WEBSOCKET_MESSAGE",
        "ENGINE_DB_LOG_PROCESS_REGISTRY",
        "ENGINE_DB_LOG_STEPS"
    ]
    
    try:
        with sqlite_connection() as conn:
            cursor = conn.cursor()
            
            print("Truncating all SQLite logging tables...")
            for table in tables:
                try:
                    cursor.execute(f"DELETE FROM {table}")
                    deleted_count = cursor.rowcount
                    print(f"  {table}: {deleted_count} rows deleted")
                except Exception as e:
                    print(f"  {table}: Error - {e}")
            
            print("All SQLite logging tables truncated successfully.")
            
    except Exception as e:
        print(f"SQLite truncate operation failed: {e}")
        print("Tables may not exist yet or database connection failed.")
