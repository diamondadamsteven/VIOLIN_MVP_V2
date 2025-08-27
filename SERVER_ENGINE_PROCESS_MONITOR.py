# SERVER_ENGINE_PROCESS_MONITOR.py
from __future__ import annotations

import os
import psutil
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import signal
import sys
import atexit

from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG, DB_INSERT_TABLE
from SERVER_ENGINE_APP_VARIABLES import ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY

# ---------------------------------------------------------------------
# Process Registry and Monitoring
# ---------------------------------------------------------------------

class ProcessMonitor:
    """Monitors server processes and manages cleanup of orphaned resources"""
    
    def __init__(self):
        self.process_id = os.getpid()
        self.start_time = datetime.now()
        self.last_heartbeat = datetime.now()
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.is_shutting_down = False
        
        # Register cleanup handlers
        atexit.register(self.emergency_cleanup)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGABRT, self.signal_handler)
    
    def register_task(self, task_name: str, task: asyncio.Task) -> None:
        """Register an active asyncio task"""
        self.active_tasks[task_name] = task
        CONSOLE_LOG("PROCESS_MONITOR", f"Registered task: {task_name}")
    
    def unregister_task(self, task_name: str) -> None:
        """Unregister a completed task"""
        if task_name in self.active_tasks:
            del self.active_tasks[task_name]
            CONSOLE_LOG("PROCESS_MONITOR", f"Unregistered task: {task_name}")
    
    async def update_heartbeat(self) -> None:
        """Update the process heartbeat in database"""
        try:
            self.last_heartbeat = datetime.now()
            
            # Store process metadata in database
            process_metadata = {
                "process_id": self.process_id,
                "start_time": self.start_time,
                "last_heartbeat": self.last_heartbeat,
                "active_tasks": str(list(self.active_tasks.keys())),  # Convert list to string for DB
                "database_connections": 5,  # TODO: Get actual count from pool
                "memory_usage_mb": round(psutil.Process().memory_info().rss / 1024 / 1024, 2),
                "cpu_percent": round(psutil.Process().cpu_percent(), 2)
            }
            
            # Upsert process registry (create table if needed)
            DB_INSERT_TABLE("ENGINE_DB_LOG_PROCESS_REGISTRY", process_metadata, fire_and_forget=True)
            
        except Exception as e:
            CONSOLE_LOG("PROCESS_MONITOR", f"Error updating heartbeat: {e}")
    
    async def cleanup_orphaned_processes(self) -> None:
        """Clean up any orphaned processes from previous runs"""
        try:
            CONSOLE_LOG("PROCESS_MONITOR", "Checking for orphaned processes...")
            
            # Look for processes with old heartbeats (older than 5 minutes)
            cutoff_time = datetime.now() - timedelta(minutes=5)
            
            # TODO: Query database for orphaned processes
            # SELECT * FROM ENGINE_DB_LOG_PROCESS_REGISTRY 
            # WHERE last_heartbeat < cutoff_time AND process_id != current_process_id
            
            # For now, just log that we're checking
            CONSOLE_LOG("PROCESS_MONITOR", "Process cleanup check completed")
            
        except Exception as e:
            CONSOLE_LOG("PROCESS_MONITOR", f"Error during orphaned process cleanup: {e}")
    
    async def graceful_shutdown(self) -> None:
        """Gracefully shut down all registered tasks"""
        if self.is_shutting_down:
            return
            
        self.is_shutting_down = True
        CONSOLE_LOG("PROCESS_MONITOR", "=== Graceful shutdown initiated ===")
        
        try:
            # Cancel all active tasks
            for task_name, task in self.active_tasks.items():
                if not task.done():
                    CONSOLE_LOG("PROCESS_MONITOR", f"Cancelling task: {task_name}")
                    task.cancel()
            
            # Wait for tasks to finish cancelling
            if self.active_tasks:
                await asyncio.gather(*self.active_tasks.values(), return_exceptions=True)
            
            # Final heartbeat update
            await self.update_heartbeat()
            
            CONSOLE_LOG("PROCESS_MONITOR", "=== Graceful shutdown complete ===")
            
        except Exception as e:
            CONSOLE_LOG("PROCESS_MONITOR", f"Error during graceful shutdown: {e}")
            self.emergency_cleanup()
    
    def emergency_cleanup(self) -> None:
        """Emergency cleanup that runs even on crashes"""
        if self.is_shutting_down:
            return
            
        self.is_shutting_down = True
        CONSOLE_LOG("PROCESS_MONITOR", "=== Emergency cleanup triggered ===")
        
        try:
            # Force cancel any remaining tasks
            for task_name, task in self.active_tasks.items():
                if not task.done():
                    task.cancel()
            
            # Log the emergency cleanup
            CONSOLE_LOG("PROCESS_MONITOR", "Emergency cleanup completed")
            
        except Exception as e:
            CONSOLE_LOG("PROCESS_MONITOR", f"Error during emergency cleanup: {e}")
    
    def signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals"""
        CONSOLE_LOG("PROCESS_MONITOR", f"Received signal {signum}, initiating cleanup...")
        asyncio.create_task(self.graceful_shutdown())
        sys.exit(0)

# ---------------------------------------------------------------------
# Global Process Monitor Instance
# ---------------------------------------------------------------------

PROCESS_MONITOR = ProcessMonitor()

# ---------------------------------------------------------------------
# Background Heartbeat Task
# ---------------------------------------------------------------------

async def PROCESS_MONITOR_HEARTBEAT() -> None:
    """Background task that updates process heartbeat every 30 seconds"""
    CONSOLE_LOG("PROCESS_MONITOR", "=== Heartbeat monitor starting ===")
    
    while not PROCESS_MONITOR.is_shutting_down:
        try:
            await PROCESS_MONITOR.update_heartbeat()
            await asyncio.sleep(30)  # Update every 30 seconds
            
        except asyncio.CancelledError:
            CONSOLE_LOG("PROCESS_MONITOR", "Heartbeat monitor cancelled")
            break
        except Exception as e:
            CONSOLE_LOG("PROCESS_MONITOR", f"Error in heartbeat monitor: {e}")
            await asyncio.sleep(5)  # Wait before retry
    
    CONSOLE_LOG("PROCESS_MONITOR", "=== Heartbeat monitor stopped ===")
