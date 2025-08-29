#!/usr/bin/env python3
"""
Create SQLite Database and Tables for VIOLIN_MVP_V2 Logging

This script creates the SQLite database file and all ENGINE_DB_LOG_* tables
that mirror the SQL Server logging schema. Run this once to set up the database,
or run with --truncate to clear all tables on startup.

Usage:
    python SERVER_ENGINE_CREATE_SQLITE_DB_AND_TABLES.py
    python SERVER_ENGINE_CREATE_SQLITE_DB_AND_TABLES.py --truncate
"""

import sqlite3
import os
import sys
from datetime import datetime

# Database path
SQLITE_DB_PATH = r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db"

def truncate_all_tables(cursor):
    """Truncate all ENGINE_DB_LOG_* tables."""
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

    print("Truncating all logging tables...")
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            deleted_count = cursor.rowcount
            print(f"  {table}: {deleted_count} rows deleted")
        except Exception as e:
            print(f"  {table}: Error - {e}")
    
    print("All tables truncated successfully.")

def create_database_and_tables(truncate_on_startup=False):
    """Create the SQLite database and all tables."""
    
    # Create database directory if it doesn't exist
    db_dir = os.path.dirname(SQLITE_DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Created directory: {db_dir}")
    
    # Connect to SQLite (creates file if it doesn't exist)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    
    print(f"Connected to SQLite database: {SQLITE_DB_PATH}")
    
    # If truncate on startup is requested, do it first
    if truncate_on_startup:
        truncate_all_tables(cursor)
        conn.commit()
        print("Database truncated and ready for new session.")
        return
    
    tables_to_drop = [
        "ENGINE_DB_LOG_FUNCTIONS",
        "ENGINE_DB_LOG_FUNCTION_ERROR", 
        "ENGINE_DB_LOG_AUDIO_FRAME_METADATA",
        "ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME",
        "ENGINE_DB_LOG_RECORDING_CONFIG",
        "ENGINE_DB_LOG_WEBSOCKET_CONNECTION",
        "ENGINE_DB_LOG_WEBSOCKET_MESSAGE",
        "ENGINE_DB_LOG_PROCESS_REGISTRY",
        "ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME",
        "ENGINE_DB_LOG_STEPS",
        "ENGINE_DB_LOG_RESOURCE_MONITOR"
    ]
    
    for table in tables_to_drop:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped table: {table}")
        except Exception as e:
            print(f"Error dropping table {table}: {e}")
            

    # Create ENGINE_DB_LOG_FUNCTIONS table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_FUNCTIONS (
            DT_ADDED DATETIME,
            PYTHON_FUNCTION_NAME VARCHAR(100),
            PYTHON_FILE_NAME VARCHAR(100),
            RECORDING_ID BIGINT,
            AUDIO_CHUNK_NO INTEGER,
            FRAME_NO INTEGER,
            START_STOP_OR_ERROR_MSG VARCHAR(500),
            WEBSOCKET_CONNECTION_ID INTEGER,
            DT_FUNCTION_MESSAGE_QUEUED DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_functions_dt_added ON ENGINE_DB_LOG_FUNCTIONS (DT_ADDED DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_functions_recording_id ON ENGINE_DB_LOG_FUNCTIONS (RECORDING_ID)")
    print("✓ ENGINE_DB_LOG_FUNCTIONS table created")

    # Create ENGINE_DB_LOG_FUNCTION_ERROR table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_FUNCTION_ERROR (
            DT_ADDED DATETIME,
            PYTHON_FUNCTION_NAME VARCHAR(100),
            PYTHON_FILE_NAME VARCHAR(100),
            ERROR_MESSAGE_TEXT VARCHAR(1000),
            TRACEBACK_TEXT VARCHAR(4000),
            RECORDING_ID BIGINT,
            AUDIO_FRAME_NO INTEGER,
            AUDIO_CHUNK_NO INTEGER,
            START_MS INTEGER,
            END_MS INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_function_error_dt_added ON ENGINE_DB_LOG_FUNCTION_ERROR (DT_ADDED DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_function_error_recording_id ON ENGINE_DB_LOG_FUNCTION_ERROR (RECORDING_ID)")
    print("✓ ENGINE_DB_LOG_FUNCTION_ERROR table created")

    # Create ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME (
            RECORDING_ID BIGINT,
            AUDIO_FRAME_NO INTEGER,
            START_MS INTEGER,
            END_MS INTEGER,
            DT_FRAME_RECEIVED DATETIME,
            DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA DATETIME,
            AUDIO_FRAME_SIZE_BYTES INTEGER,
            AUDIO_FRAME_ENCODING VARCHAR(20),
            AUDIO_FRAME_SHA256_HEX VARCHAR(100),
            WEBSOCKET_CONNECTION_ID INTEGER,
            PRE_SPLIT_AUDIO_FRAME_DURATION_IN_MS INTEGER,
            DT_FRAME_SPLIT_INTO_100_MS_FRAMES DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_pre_split_recording_id ON ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME (RECORDING_ID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_pre_split_frame_no ON ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME (AUDIO_FRAME_NO)")
    print("✓ ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME table created")

    # Create ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME (
            RECORDING_ID BIGINT,
            AUDIO_FRAME_NO INTEGER,
            START_MS INTEGER,
            END_MS INTEGER,
            YN_RUN_FFT CHAR(1),
            YN_RUN_ONS CHAR(1),
            YN_RUN_PYIN CHAR(1),
            YN_RUN_CREPE CHAR(1),
            DT_FRAME_RECEIVED DATETIME,
            DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA DATETIME,
            DT_FRAME_DECODED_FROM_BASE64_TO_BYTES DATETIME,
            DT_FRAME_CONVERTED_TO_PCM16_WITH_SAMPLE_RATE_44100 DATETIME,
            DT_FRAME_APPENDED_TO_RAW_FILE DATETIME,
            DT_FRAME_RESAMPLED_TO_16000 DATETIME,
            DT_FRAME_RESAMPLED_22050 DATETIME,
            DT_PROCESSING_START DATETIME,
            DT_PROCESSING_END DATETIME,
            DT_START_FFT DATETIME,
            DT_END_FFT DATETIME,
            DT_START_ONS DATETIME,
            DT_END_ONS DATETIME,
            DT_START_PYIN DATETIME,
            DT_END_PYIN DATETIME,
            DT_START_CREPE DATETIME,
            DT_END_CREPE DATETIME,
            DT_START_VOLUME_1_MS DATETIME,
            DT_END_VOLUME_1_MS DATETIME,
            DT_START_VOLUME_10_MS DATETIME,
            DT_END_VOLUME_10_MS DATETIME,
            FFT_RECORD_CNT INTEGER,
            ONS_RECORD_CNT INTEGER,
            PYIN_RECORD_CNT INTEGER,
            CREPE_RECORD_CNT INTEGER,
            VOLUME_1_MS_RECORD_CNT INTEGER,
            VOLUME_10_MS_RECORD_CNT INTEGER,
            DT_ADDED DATETIME,
            AUDIO_FRAME_SIZE_BYTES INTEGER,
            AUDIO_FRAME_ENCODING VARCHAR(20),
            AUDIO_FRAME_SHA256_HEX VARCHAR(100),
            DT_FRAME_DECODED_FROM_BYTES_INTO_AUDIO_SAMPLES DATETIME,
            DT_FRAME_RESAMPLED_TO_44100 DATETIME,
            DT_PROCESSING_QUEUED_TO_START DATETIME,
            DT_START_PYIN_ENGINE_LOAD_HZ_INS DATETIME,
            DT_END_PYIN_ENGINE_LOAD_HZ_INS DATETIME,
            DT_START_PYIN_RELATIVE_ROWS DATETIME,
            DT_END_PYIN_RELATIVE_ROWS DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_split_recording_id ON ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME (RECORDING_ID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_split_frame_no ON ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME (AUDIO_FRAME_NO)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_split_start_ms ON ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME (START_MS)")
    print("✓ ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME table created")

    # Create ENGINE_DB_LOG_RECORDING_CONFIG table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_RECORDING_CONFIG (
            RECORDING_ID BIGINT,
            DT_RECORDING_START DATETIME,
            COMPOSE_PLAY_OR_PRACTICE VARCHAR(50),
            AUDIO_STREAM_FILE_NAME VARCHAR(100),
            COMPOSE_YN_RUN_FFT CHAR(1),
            DT_ADDED DATETIME,
            WEBSOCKET_CONNECTION_ID INTEGER,
            DT_RECORDING_END DATETIME,
            DT_RECORDING_DATA_PURGED DATETIME,
            DT_RECORDING_DATA_QUEUED_FOR_PURGING DATETIME,
            DT_PROCESS_WEBSOCKET_START_MESSAGE_DONE DATETIME,
            MAX_PRE_SPLIT_AUDIO_FRAME_NO_SPLIT INTEGER,
            TOTAL_BYTES_RECEIVED BIGINT,
            TOTAL_SPLIT_100_MS_FRAMES_PRODUCED INTEGER,
            SPLIT_100_MS_FRAME_COUNTER INTEGER,
            LAST_SPLIT_100_MS_FRAME_TIME DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_recording_config_recording_id ON ENGINE_DB_LOG_RECORDING_CONFIG (RECORDING_ID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_recording_config_dt_start ON ENGINE_DB_LOG_RECORDING_CONFIG (DT_RECORDING_START)")
    print("✓ ENGINE_DB_LOG_RECORDING_CONFIG table created")

    # Create ENGINE_DB_LOG_WEBSOCKET_CONNECTION table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_WEBSOCKET_CONNECTION (
            WEBSOCKET_CONNECTION_ID INTEGER,
            CLIENT_HOST_IP_ADDRESS VARCHAR(50),
            CLIENT_PORT VARCHAR(10),
            CLIENT_HEADERS VARCHAR(2000),
            DT_CONNECTION_REQUEST DATETIME,
            DT_CONNECTION_ACCEPTED DATETIME,
            DT_CONNECTION_CLOSED DATETIME,
            DT_WEBSOCKET_DISCONNECT_EVENT DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_websocket_connection_id ON ENGINE_DB_LOG_WEBSOCKET_CONNECTION (WEBSOCKET_CONNECTION_ID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_websocket_connection_dt_request ON ENGINE_DB_LOG_WEBSOCKET_CONNECTION (DT_CONNECTION_REQUEST)")
    print("✓ ENGINE_DB_LOG_WEBSOCKET_CONNECTION table created")

    # Create ENGINE_DB_LOG_WEBSOCKET_MESSAGE table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_WEBSOCKET_MESSAGE (
            RECORDING_ID BIGINT,
            MESSAGE_TYPE VARCHAR(50),
            AUDIO_FRAME_NO INTEGER,
            DT_MESSAGE_RECEIVED DATETIME,
            DT_MESSAGE_PROCESS_STARTED DATETIME,
            WEBSOCKET_CONNECTION_ID INTEGER,
            MESSAGE_ID INTEGER,
            DT_MESSAGE_PROCESS_QUEUED_TO_START DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_websocket_message_recording_id ON ENGINE_DB_LOG_WEBSOCKET_MESSAGE (RECORDING_ID)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_websocket_message_dt_received ON ENGINE_DB_LOG_WEBSOCKET_MESSAGE (DT_MESSAGE_RECEIVED)")
    print("✓ ENGINE_DB_LOG_WEBSOCKET_MESSAGE table created")

    # Create ENGINE_DB_LOG_PROCESS_REGISTRY table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_PROCESS_REGISTRY (
            process_id INTEGER,
            start_time DATETIME,
            last_heartbeat DATETIME,
            active_tasks VARCHAR(1000),
            database_connections INTEGER,
            memory_usage_mb REAL,
            cpu_percent REAL,
            dt_added DATETIME
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_process_registry_process_id ON ENGINE_DB_LOG_PROCESS_REGISTRY (process_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_process_registry_dt_added ON ENGINE_DB_LOG_PROCESS_REGISTRY (dt_added)")
    print("✓ ENGINE_DB_LOG_PROCESS_REGISTRY table created")

    # Create ENGINE_DB_LOG_STEPS table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_STEPS (
            DT_ADDED DATETIME,
            STEP_NAME VARCHAR(100),
            PYTHON_FUNCTION_NAME VARCHAR(100),
            PYTHON_FILE_NAME VARCHAR(100),
            RECORDING_ID BIGINT,
            AUDIO_CHUNK_NO INTEGER,
            FRAME_NO INTEGER,
            DT_STEP_CALLED DATETIME,
            STEP_ID INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_steps_dt_added ON ENGINE_DB_LOG_STEPS (DT_ADDED)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_steps_recording_id ON ENGINE_DB_LOG_STEPS (RECORDING_ID)")
    print("✓ ENGINE_DB_LOG_STEPS table created")

    # Create ENGINE_DB_LOG_RESOURCE_MONITOR table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ENGINE_DB_LOG_RESOURCE_MONITOR (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            DT_MEASUREMENT DATETIME,
            CPU_PERCENT REAL,
            MEMORY_PERCENT REAL,
            DISK_IO_TOTAL_BYTES BIGINT,
            THREAD_COUNT INTEGER,
            MEMORY_AVAILABLE_GB REAL,
            DT_ADDED DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_resource_monitor_dt_measurement ON ENGINE_DB_LOG_RESOURCE_MONITOR (DT_MEASUREMENT DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_resource_monitor_cpu_percent ON ENGINE_DB_LOG_RESOURCE_MONITOR (CPU_PERCENT)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_resource_monitor_memory_percent ON ENGINE_DB_LOG_RESOURCE_MONITOR (MEMORY_PERCENT)")
    print("✓ ENGINE_DB_LOG_RESOURCE_MONITOR table created")

    # Commit all changes
    conn.commit()
    print("\n✓ All tables created successfully!")
    
    # Show table info
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    print(f"\nTables in database: {len(tables)}")
    for table in tables:
        print(f"  - {table[0]}")
    
    conn.close()
    print(f"\nDatabase ready: {SQLITE_DB_PATH}")

if __name__ == "__main__":
    truncate_on_startup = "--truncate" in sys.argv
    
    if truncate_on_startup:
        print("=== SQLite Database Setup with Truncate on Startup ===")
        print(f"Database: {SQLITE_DB_PATH}")
        print(f"Timestamp: {datetime.now()}")
        print()
        create_database_and_tables(truncate_on_startup=True)
    else:
        print("=== SQLite Database Setup ===")
        print(f"Database: {SQLITE_DB_PATH}")
        print(f"Timestamp: {datetime.now()}")
        print()
        create_database_and_tables(truncate_on_startup=False)
