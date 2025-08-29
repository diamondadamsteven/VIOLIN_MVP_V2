#!/usr/bin/env python3
"""
SQLite Logging Database Schema Manager for VIOLIN_MVP_V2

This script creates and manages the SQLite logging database that will replace
SQL Server for logging operations, improving audio processing performance.

Run this script when you want to:
- Create the initial database
- Add new logging tables
- Modify existing table schemas
- Reset the database if needed

Usage: python create_sqlite_logging_db.py
"""

import sqlite3
import os
from datetime import datetime

def create_sqlite_logging_database():
    """Create the SQLite logging database with all ENGINE_DB_LOG* tables."""
    
    db_path = r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db"
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Creating SQLite logging database: {db_path}")
    
    # 1. ENGINE_DB_LOG_FUNCTION_ERROR
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_FUNCTION_ERROR (
            DT_ADDED TEXT,
            PYTHON_FUNCTION_NAME TEXT,
            PYTHON_FILE_NAME TEXT,
            ERROR_MESSAGE_TEXT TEXT,
            TRACEBACK_TEXT TEXT,
            RECORDING_ID INTEGER,
            AUDIO_FRAME_NO INTEGER,
            AUDIO_CHUNK_NO INTEGER,
            START_MS INTEGER,
            END_MS INTEGER
        )
    """)
    
    # 2. ENGINE_DB_LOG_FUNCTIONS
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_FUNCTIONS (
            DT_ADDED TEXT,
            PYTHON_FUNCTION_NAME TEXT,
            PYTHON_FILE_NAME TEXT,
            RECORDING_ID INTEGER,
            AUDIO_CHUNK_NO INTEGER,
            FRAME_NO INTEGER,
            START_STOP_OR_ERROR_MSG TEXT,
            WEBSOCKET_CONNECTION_ID INTEGER,
            DT_FUNCTION_MESSAGE_QUEUED TEXT
        )
    """)
    
    # 3. ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME (
            RECORDING_ID INTEGER,
            AUDIO_FRAME_NO INTEGER,
            START_MS INTEGER,
            END_MS INTEGER,
            DT_FRAME_RECEIVED TEXT,
            DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA TEXT,
            AUDIO_FRAME_SIZE_BYTES INTEGER,
            AUDIO_FRAME_ENCODING TEXT,
            AUDIO_FRAME_SHA256_HEX TEXT,
            WEBSOCKET_CONNECTION_ID INTEGER,
            PRE_SPLIT_AUDIO_FRAME_DURATION_IN_MS INTEGER,
            DT_FRAME_SPLIT_INTO_100_MS_FRAMES TEXT
        )
    """)
    
    # 4. ENGINE_DB_LOG_PROCESS_REGISTRY
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_PROCESS_REGISTRY (
            process_id INTEGER NOT NULL,
            start_time TEXT,
            last_heartbeat TEXT,
            active_tasks TEXT,
            database_connections INTEGER,
            memory_usage_mb REAL,
            cpu_percent REAL,
            dt_added TEXT DEFAULT (datetime('now'))
        )
    """)
    
    # 5. ENGINE_DB_LOG_RECORDING_CONFIG
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_RECORDING_CONFIG (
            RECORDING_ID INTEGER,
            DT_RECORDING_START TEXT,
            COMPOSE_PLAY_OR_PRACTICE TEXT,
            AUDIO_STREAM_FILE_NAME TEXT,
            COMPOSE_YN_RUN_FFT TEXT,
            DT_ADDED TEXT,
            WEBSOCKET_CONNECTION_ID INTEGER,
            DT_RECORDING_END TEXT,
            DT_RECORDING_DATA_PURGED TEXT,
            DT_RECORDING_DATA_QUEDED_FOR_PURGING TEXT,
            DT_PROCESS_WEBSOCKET_START_MESSAGE_DONE TEXT,
            MAX_PRE_SPLIT_AUDIO_FRAME_NO_SPLIT INTEGER,
            TOTAL_BYTES_RECEIVED INTEGER,
            TOTAL_SPLIT_100_MS_FRAMES_PRODUCED INTEGER,
            SPLIT_100_MS_FRAME_COUNTER INTEGER,
            LAST_SPLIT_100_MS_FRAME_TIME TEXT
        )
    """)
    
    # 6. ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME (
            RECORDING_ID INTEGER,
            AUDIO_FRAME_NO INTEGER,
            START_MS INTEGER,
            END_MS INTEGER,
            YN_RUN_FFT TEXT,
            YN_RUN_ONS TEXT,
            YN_RUN_PYIN TEXT,
            YN_RUN_CREPE TEXT,
            DT_FRAME_RECEIVED TEXT,
            DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA TEXT,
            DT_FRAME_DECODED_FROM_BASE64_TO_BYTES TEXT,
            DT_FRAME_CONVERTED_TO_PCM16_WITH_SAMPLE_RATE_44100 TEXT,
            DT_FRAME_APPENDED_TO_RAW_FILE TEXT,
            DT_FRAME_RESAMPLED_TO_16000 TEXT,
            DT_FRAME_RESAMPLED_22050 TEXT,
            DT_PROCESSING_START TEXT,
            DT_PROCESSING_END TEXT,
            DT_START_FFT TEXT,
            DT_END_FFT TEXT,
            DT_START_ONS TEXT,
            DT_END_ONS TEXT,
            DT_START_PYIN TEXT,
            DT_END_PYIN TEXT,
            DT_START_CREPE TEXT,
            DT_END_CREPE TEXT,
            DT_START_VOLUME_1_MS TEXT,
            DT_END_VOLUME_1_MS TEXT,
            DT_START_VOLUME_10_MS TEXT,
            DT_END_VOLUME_10_MS TEXT,
            FFT_RECORD_CNT INTEGER,
            ONS_RECORD_CNT INTEGER,
            PYIN_RECORD_CNT INTEGER,
            CREPE_RECORD_CNT INTEGER,
            VOLUME_1_MS_RECORD_CNT INTEGER,
            VOLUME_10_MS_RECORD_CNT INTEGER,
            DT_ADDED TEXT,
            AUDIO_FRAME_SIZE_BYTES INTEGER,
            AUDIO_FRAME_ENCODING TEXT,
            AUDIO_FRAME_SHA256_HEX TEXT,
            DT_FRAME_DECODED_FROM_BYTES_INTO_AUDIO_SAMPLES TEXT,
            DT_FRAME_RESAMPLED_TO_44100 TEXT,
            DT_PROCESSING_QUEUED_TO_START TEXT
        )
    """)
    
    # 7. ENGINE_DB_LOG_STEPS
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_STEPS (
            DT_ADDED TEXT,
            STEP_NAME TEXT,
            PYTHON_FUNCTION_NAME TEXT,
            PYTHON_FILE_NAME TEXT,
            RECORDING_ID INTEGER,
            AUDIO_CHUNK_NO INTEGER,
            FRAME_NO INTEGER,
            DT_STEP_CALLED TEXT,
            STEP_ID INTEGER
        )
    """)
    
    # 8. ENGINE_DB_LOG_WEBSOCKET_CONNECTION
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_WEBSOCKET_CONNECTION (
            WEBSOCKET_CONNECTION_ID INTEGER,
            CLIENT_HOST_IP_ADDRESS TEXT,
            CLIENT_PORT INTEGER,
            CLIENT_HEADERS TEXT,
            DT_CONNECTION_REQUEST TEXT,
            DT_CONNECTION_ACCEPTED TEXT,
            DT_CONNECTION_CLOSED TEXT,
            DT_WEBSOCKET_DISCONNECT_EVENT TEXT
        )
    """)
    
    # 9. ENGINE_DB_LOG_WEBSOCKET_MESSAGE
    cursor.execute("""
        CREATE TABLE ENGINE_DB_LOG_WEBSOCKET_MESSAGE (
            RECORDING_ID INTEGER,
            MESSAGE_TYPE TEXT,
            AUDIO_FRAME_NO INTEGER,
            DT_MESSAGE_RECEIVED TEXT,
            DT_MESSAGE_PROCESS_STARTED TEXT,
            WEBSOCKET_CONNECTION_ID INTEGER,
            MESSAGE_ID INTEGER,
            DT_MESSAGE_PROCESS_QUEUED_TO_START TEXT
        )
    """)
    
    # Create indexes for performance
    print("Creating performance indexes...")
    
    # ENGINE_DB_LOG_FUNCTIONS indexes
    cursor.execute("""
        CREATE INDEX idx_log_functions_recording 
        ON ENGINE_DB_LOG_FUNCTIONS(RECORDING_ID, DT_ADDED)
    """)
    
    cursor.execute("""
        CREATE INDEX idx_log_functions_dt_added 
        ON ENGINE_DB_LOG_FUNCTIONS(DT_ADDED)
    """)
    
    # ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME indexes
    cursor.execute("""
        CREATE INDEX idx_log_split_frame_recording 
        ON ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME(RECORDING_ID, AUDIO_FRAME_NO)
    """)
    
    cursor.execute("""
        CREATE INDEX idx_log_split_frame_dt_processing 
        ON ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME(DT_PROCESSING_START, DT_PROCESSING_END)
    """)
    
    # ENGINE_DB_LOG_RECORDING_CONFIG indexes
    cursor.execute("""
        CREATE INDEX idx_log_recording_config_id 
        ON ENGINE_DB_LOG_RECORDING_CONFIG(RECORDING_ID)
    """)
    
    # ENGINE_DB_LOG_WEBSOCKET indexes
    cursor.execute("""
        CREATE INDEX idx_log_websocket_connection_id 
        ON ENGINE_DB_LOG_WEBSOCKET_CONNECTION(WEBSOCKET_CONNECTION_ID)
    """)
    
    cursor.execute("""
        CREATE INDEX idx_log_websocket_message_recording 
        ON ENGINE_DB_LOG_WEBSOCKET_MESSAGE(RECORDING_ID, AUDIO_FRAME_NO)
    """)
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Successfully created SQLite logging database: {db_path}")
    print("\nTables created:")
    print("1. ENGINE_DB_LOG_FUNCTION_ERROR")
    print("2. ENGINE_DB_LOG_FUNCTIONS")
    print("3. ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME")
    print("4. ENGINE_DB_LOG_PROCESS_REGISTRY")
    print("5. ENGINE_DB_LOG_RECORDING_CONFIG")
    print("6. ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME")
    print("7. ENGINE_DB_LOG_STEPS")
    print("8. ENGINE_DB_LOG_WEBSOCKET_CONNECTION")
    print("9. ENGINE_DB_LOG_WEBSOCKET_MESSAGE")
    print("\nPerformance indexes created for key columns.")
    print("\nNext steps:")
    print("1. Set up SQL Server linked server to this SQLite database")
    print("2. Modify logging functions to write to SQLite instead of SQL Server")
    print("3. Create SQL Server views that read from SQLite")

def verify_database_exists():
    """Verify that the SQLite database exists and has the expected tables."""
    
    db_path = r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        print("Run this script to create the database.")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        expected_tables = [
            'ENGINE_DB_LOG_FUNCTION_ERROR',
            'ENGINE_DB_LOG_FUNCTIONS',
            'ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME',
            'ENGINE_DB_LOG_PROCESS_REGISTRY',
            'ENGINE_DB_LOG_RECORDING_CONFIG',
            'ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME',
            'ENGINE_DB_LOG_STEPS',
            'ENGINE_DB_LOG_WEBSOCKET_CONNECTION',
            'ENGINE_DB_LOG_WEBSOCKET_MESSAGE'
        ]
        
        missing_tables = [t for t in expected_tables if t not in tables]
        
        if missing_tables:
            print(f"❌ Missing tables: {missing_tables}")
            return False
        
        print(f"✅ Database verified: {db_path}")
        print(f"✅ All {len(tables)} expected tables found")
        return True
        
    except Exception as e:
        print(f"❌ Error verifying database: {e}")
        return False

if __name__ == "__main__":
    print("VIOLIN_MVP_V2 SQLite Logging Database Manager")
    print("=" * 50)
    
    # Check if database already exists
    if os.path.exists(r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db"):
        print("Database already exists. Do you want to recreate it? (y/n): ", end="")
        response = input().lower().strip()
        
        if response == 'y':
            create_sqlite_logging_database()
        else:
            print("Database creation skipped.")
            verify_database_exists()
    else:
        create_sqlite_logging_database()
    
    print("\n" + "=" * 50)
    print("Script completed.")
