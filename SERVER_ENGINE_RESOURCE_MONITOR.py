"""
Resource monitoring module for VIOLIN MVP to detect resource contention during runtime.
This module monitors CPU, memory, disk I/O, and thread usage to identify bottlenecks.
"""

import os
import time
import threading
import psutil
import numpy as np
from typing import Dict, List, Optional, Tuple
from collections import deque
import logging
from datetime import datetime
import sqlite3

# Configure logging
LOGGER = logging.getLogger(__name__)

class ResourceMonitor:
    """Monitors system resources to detect contention during audio processing."""
    
    def __init__(self, history_size: int = 1000):
        self.history_size = history_size
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Resource history (circular buffers)
        self.cpu_history = deque(maxlen=history_size)
        self.memory_history = deque(maxlen=history_size)
        self.disk_io_history = deque(maxlen=history_size)
        self.thread_history = deque(maxlen=history_size)
        self.timestamp_history = deque(maxlen=history_size)
        
        # Baseline measurements
        self.baseline_cpu = 0.0
        self.baseline_memory = 0.0
        self.baseline_disk_io = 0.0
        self.baseline_threads = 0
        
        # Contention thresholds
        self.cpu_threshold = 80.0  # CPU usage above 80% indicates contention
        self.memory_threshold = 85.0  # Memory usage above 85% indicates contention
        self.disk_io_threshold = 2.0  # Disk I/O wait above 2% indicates contention
        self.thread_threshold = 1.5  # Thread count 1.5x baseline indicates contention
        
        # Contention alerts
        self.contention_alerts: List[Dict] = []
        
        # Database connection for logging
        self.db_path = r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db"
        
    def start_monitoring(self, interval_seconds: float = 0.1) -> None:
        """Start resource monitoring."""
        if self.monitoring:
            LOGGER.warning("Resource monitoring already running")
            return
        
        LOGGER.info("Starting resource monitoring...")
        
        # Establish baseline measurements
        self._establish_baseline()
        
        # Start monitoring thread
        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval_seconds,),
            daemon=True,
            name="ResourceMonitor"
        )
        self.monitor_thread.start()
        
        LOGGER.info("Resource monitoring started successfully")
    
    def stop_monitoring(self) -> None:
        """Stop resource monitoring."""
        if not self.monitoring:
            return
        
        LOGGER.info("Stopping resource monitoring...")
        self.monitoring = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        
        LOGGER.info("Resource monitoring stopped")
    
    def _establish_baseline(self) -> None:
        """Establish baseline resource usage measurements."""
        LOGGER.info("Establishing resource usage baseline...")
        
        # Take multiple measurements and average them
        cpu_samples = []
        memory_samples = []
        disk_io_samples = []
        thread_samples = []
        
        for _ in range(10):
            cpu_samples.append(psutil.cpu_percent(interval=0.1))
            memory_samples.append(psutil.virtual_memory().percent)
            disk_io = psutil.disk_io_counters()
            disk_io_samples.append(disk_io.read_bytes + disk_io.write_bytes)
            thread_samples.append(psutil.Process().num_threads())
            time.sleep(0.1)
        
        self.baseline_cpu = np.mean(cpu_samples)
        self.baseline_memory = np.mean(memory_samples)
        self.baseline_disk_io = np.mean(disk_io_samples)
        self.baseline_threads = np.mean(thread_samples)
        
        LOGGER.info(f"Baseline established:")
        LOGGER.info(f"  CPU: {self.baseline_cpu:.1f}%")
        LOGGER.info(f"  Memory: {self.baseline_memory:.1f}%")
        LOGGER.info(f"  Disk I/O: {self.baseline_disk_io / 1024 / 1024:.1f} MB/s")
        LOGGER.info(f"  Threads: {self.baseline_threads:.1f}")
    
    def _monitor_loop(self, interval_seconds: float) -> None:
        """Main monitoring loop."""
        while self.monitoring:
            try:
                self._collect_measurements()
                self._detect_contention()
                time.sleep(interval_seconds)
            except Exception as e:
                LOGGER.error(f"Error in monitoring loop: {e}")
                time.sleep(1.0)  # Longer sleep on error
    
    def _collect_measurements(self) -> None:
        """Collect current resource measurements."""
        timestamp = datetime.now()
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.01)
        
        # Memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_available_gb = memory.available / (1024**3)
        
        # Disk I/O
        disk_io = psutil.disk_io_counters()
        disk_io_total = disk_io.read_bytes + disk_io.write_bytes
        
        # Thread count
        thread_count = psutil.Process().num_threads()
        
        # Store measurements
        self.cpu_history.append(cpu_percent)
        self.memory_history.append(memory_percent)
        self.disk_io_history.append(disk_io_total)
        self.thread_history.append(thread_count)
        self.timestamp_history.append(timestamp)
        
        # Log to database
        self._log_measurement(timestamp, cpu_percent, memory_percent, 
                             disk_io_total, thread_count, memory_available_gb)
    
    def _detect_contention(self) -> None:
        """Detect resource contention based on current measurements."""
        if not self.cpu_history or not self.memory_history:
            return
        
        current_cpu = self.cpu_history[-1]
        current_memory = self.memory_history[-1]
        current_threads = self.thread_history[-1]
        
        # Check for contention
        contention_detected = False
        alert = {
            "timestamp": datetime.now(),
            "type": "resource_contention",
            "details": []
        }
        
        # CPU contention
        if current_cpu > self.cpu_threshold:
            contention_detected = True
            alert["details"].append({
                "resource": "CPU",
                "current": f"{current_cpu:.1f}%",
                "baseline": f"{self.baseline_cpu:.1f}%",
                "threshold": f"{self.cpu_threshold:.1f}%"
            })
        
        # Memory contention
        if current_memory > self.memory_threshold:
            contention_detected = True
            alert["details"].append({
                "resource": "Memory",
                "current": f"{current_memory:.1f}%",
                "baseline": f"{self.baseline_memory:.1f}%",
                "threshold": f"{self.memory_threshold:.1f}%"
            })
        
        # Thread contention
        if current_threads > self.baseline_threads * self.thread_threshold:
            contention_detected = True
            alert["details"].append({
                "resource": "Threads",
                "current": f"{current_threads:.1f}",
                "baseline": f"{self.baseline_threads:.1f}",
                "threshold": f"{self.baseline_threads * self.thread_threshold:.1f}"
            })
        
        if contention_detected:
            self.contention_alerts.append(alert)
            # LOGGER.warning(f"Resource contention detected: {alert['details']}")
    
    def _log_measurement(self, timestamp: datetime, cpu_percent: float, 
                        memory_percent: float, disk_io_total: int, 
                        thread_count: int, memory_available_gb: float) -> None:
        """Log measurement to SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO ENGINE_DB_LOG_RESOURCE_MONITOR (
                        DT_MEASUREMENT, CPU_PERCENT, MEMORY_PERCENT, 
                        DISK_IO_TOTAL_BYTES, THREAD_COUNT, MEMORY_AVAILABLE_GB,
                        DT_ADDED
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    timestamp, cpu_percent, memory_percent,
                    disk_io_total, thread_count, memory_available_gb,
                    datetime.now()
                ))
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Failed to log measurement to database: {e}")
    
    def get_current_status(self) -> Dict:
        """Get current resource status."""
        if not self.cpu_history:
            return {"status": "no_data"}
        
        current_cpu = self.cpu_history[-1]
        current_memory = self.memory_history[-1]
        current_threads = self.thread_history[-1]
        
        # Calculate trends (last 10 measurements)
        recent_cpu = list(self.cpu_history)[-10:] if len(self.cpu_history) >= 10 else list(self.cpu_history)
        recent_memory = list(self.memory_history)[-10:] if len(self.memory_history) >= 10 else list(self.memory_history)
        
        cpu_trend = "stable"
        if len(recent_cpu) >= 2:
            if recent_cpu[-1] > recent_cpu[0] * 1.2:
                cpu_trend = "increasing"
            elif recent_cpu[-1] < recent_cpu[0] * 0.8:
                cpu_trend = "decreasing"
        
        memory_trend = "stable"
        if len(recent_memory) >= 2:
            if recent_memory[-1] > recent_memory[0] * 1.2:
                memory_trend = "increasing"
            elif recent_memory[-1] < recent_memory[0] * 0.8:
                memory_trend = "decreasing"
        
        return {
            "status": "monitoring",
            "timestamp": datetime.now().isoformat(),
            "cpu": {
                "current": current_cpu,
                "baseline": self.baseline_cpu,
                "trend": cpu_trend,
                "contention": current_cpu > self.cpu_threshold
            },
            "memory": {
                "current": current_memory,
                "baseline": self.baseline_memory,
                "trend": memory_trend,
                "contention": current_memory > self.memory_threshold
            },
            "threads": {
                "current": current_threads,
                "baseline": self.baseline_threads,
                "contention": current_threads > self.baseline_threads * self.thread_threshold
            },
            "alerts_count": len(self.contention_alerts)
        }
    
    def get_contention_summary(self) -> Dict:
        """Get summary of resource contention."""
        if not self.contention_alerts:
            return {"contention_detected": False}
        
        # Group alerts by resource type
        resource_alerts = {}
        for alert in self.contention_alerts:
            for detail in alert["details"]:
                resource = detail["resource"]
                if resource not in resource_alerts:
                    resource_alerts[resource] = []
                resource_alerts[resource].append({
                    "timestamp": alert["timestamp"],
                    "current": detail["current"],
                    "baseline": detail["baseline"]
                })
        
        return {
            "contention_detected": True,
            "total_alerts": len(self.contention_alerts),
            "resource_breakdown": resource_alerts
        }
    
    def get_performance_metrics(self, window_minutes: int = 5) -> Dict:
        """Get performance metrics over a time window."""
        if not self.timestamp_history:
            return {"error": "no_data"}
        
        # Calculate time window
        window_start = datetime.now().timestamp() - (window_minutes * 60)
        
        # Filter measurements within window
        recent_measurements = []
        for i, timestamp in enumerate(self.timestamp_history):
            if timestamp.timestamp() >= window_start:
                recent_measurements.append({
                    "timestamp": timestamp,
                    "cpu": self.cpu_history[i],
                    "memory": self.memory_history[i],
                    "threads": self.thread_history[i]
                })
        
        if not recent_measurements:
            return {"error": "no_data_in_window"}
        
        # Calculate statistics
        cpu_values = [m["cpu"] for m in recent_measurements]
        memory_values = [m["memory"] for m in recent_measurements]
        thread_values = [m["threads"] for m in recent_measurements]
        
        return {
            "window_minutes": window_minutes,
            "measurements_count": len(recent_measurements),
            "cpu": {
                "min": min(cpu_values),
                "max": max(cpu_values),
                "mean": np.mean(cpu_values),
                "std": np.std(cpu_values)
            },
            "memory": {
                "min": min(memory_values),
                "max": max(memory_values),
                "mean": np.mean(memory_values),
                "std": np.std(memory_values)
            },
            "threads": {
                "min": min(thread_values),
                "max": max(thread_values),
                "mean": np.mean(thread_values),
                "std": np.std(thread_values)
            }
        }

# Global instance
RESOURCE_MONITOR = ResourceMonitor()

def start_resource_monitoring(interval_seconds: float = 0.1) -> None:
    """Global function to start resource monitoring."""
    RESOURCE_MONITOR.start_monitoring(interval_seconds)

def stop_resource_monitoring() -> None:
    """Global function to stop resource monitoring."""
    RESOURCE_MONITOR.stop_monitoring()

def get_resource_status() -> Dict:
    """Global function to get current resource status."""
    return RESOURCE_MONITOR.get_current_status()

def get_contention_summary() -> Dict:
    """Global function to get contention summary."""
    return RESOURCE_MONITOR.get_contention_summary()

def get_performance_metrics(window_minutes: int = 5) -> Dict:
    """Global function to get performance metrics."""
    return RESOURCE_MONITOR.get_performance_metrics(window_minutes)

if __name__ == "__main__":
    # Test the resource monitor
    logging.basicConfig(level=logging.INFO)
    
    try:
        start_resource_monitoring(interval_seconds=0.5)
        print("Resource monitoring started. Press Ctrl+C to stop...")
        
        # Monitor for 10 seconds
        for i in range(20):
            time.sleep(0.5)
            status = get_resource_status()
            print(f"Status: {status}")
        
    except KeyboardInterrupt:
        print("\nStopping resource monitoring...")
    finally:
        stop_resource_monitoring()
        print("Resource monitoring stopped")
