"""
Pre-warming module for VIOLIN MVP to avoid resource contention during audio processing.
This module pre-allocates memory, warms CPU caches, and initializes thread pools at startup.
"""

import os
import time
import threading
import multiprocessing
import numpy as np
import psutil
import gc
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import logging
import librosa

# Configure logging
LOGGER = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)

class ResourcePrewarmer:
    """Pre-warms system resources to avoid contention during audio processing."""
    
    def __init__(self):
        self.memory_pools: Dict[str, List[np.ndarray]] = {}
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        self.process_pool: Optional[ProcessPoolExecutor] = None
        self.cpu_cache_warmed = False
        self.memory_preallocated = False
        self.thread_pools_initialized = False
        
    def prewarm_all_resources(self) -> None:
        """Pre-warm all system resources."""
        LOGGER.warning("Starting resource pre-warming...")
        
        try:
            # 1. Pre-allocate memory pools
            self._preallocate_memory_pools()
            
            # 2. Warm CPU caches
            self._warm_cpu_caches()
            
            # 3. Initialize thread pools
            self._initialize_thread_pools()
            
            # 4. Pre-warm audio processing functions
            prewarm_pyin_engine()
            prewarm_audio_resampling()
            
            LOGGER.warning("Resource pre-warming completed successfully")
            
        except Exception as e:
            LOGGER.error(f"Resource pre-warming failed: {e}")
            raise
    
    def _preallocate_memory_pools(self) -> None:
        """Pre-allocate memory pools for audio processing."""
        LOGGER.warning("Pre-allocating memory pools...")
        
        # Audio frame sizes we'll be processing
        frame_sizes = [
            (44100, 100),   # 100ms at 44.1kHz
            (22050, 100),   # 100ms at 22.05kHz  
            (16000, 100),   # 100ms at 16kHz
            (44100, 250),   # 250ms at 44.1kHz (larger frames)
        ]
        
        # Pre-allocate arrays for each frame size
        for sample_rate, duration_ms in frame_sizes:
            samples = int(sample_rate * duration_ms / 1000)
            key = f"{sample_rate}_{duration_ms}ms"
            
            # Allocate multiple buffers to avoid allocation during processing
            self.memory_pools[key] = [
                np.zeros(samples, dtype=np.float32),  # Float32 for audio
                np.zeros(samples, dtype=np.float32),  # Backup buffer
                np.zeros(samples, dtype=np.float32),  # Another backup
            ]
            
            LOGGER.warning(f"Pre-allocated {len(self.memory_pools[key])} buffers for {key} ({samples} samples)")
        
        # Pre-allocate larger buffers for FFT processing
        fft_sizes = [1024, 2048, 4096, 8192]
        for fft_size in fft_sizes:
            key = f"fft_{fft_size}"
            self.memory_pools[key] = [
                np.zeros(fft_size, dtype=np.complex64),  # Complex for FFT
                np.zeros(fft_size, dtype=np.complex64),
            ]
            LOGGER.warning(f"Pre-allocated FFT buffers for size {fft_size}")
        
        self.memory_preallocated = True
        LOGGER.warning("Memory pools pre-allocated successfully")
    
    def _warm_cpu_caches(self) -> None:
        """Warm CPU caches by running sample computations."""
        LOGGER.warning("Warming CPU caches...")
        
        # Create sample data
        sample_data = np.random.random(10000).astype(np.float32)
        
        # Warm L1/L2 cache with repeated operations
        for _ in range(1000):
            # Simple arithmetic operations to warm cache
            result = sample_data * 2.0 + 1.0
            result = np.sqrt(result)
            result = np.sin(result)
        
        # Warm with FFT-like operations (similar to what we'll do in audio processing)
        for size in [1024, 2048, 4096]:
            test_data = np.random.random(size).astype(np.float32)
            for _ in range(100):
                # Simulate FFT-like memory access patterns
                result = test_data[::2] + test_data[1::2]  # Decimation
                result = result * np.hanning(len(result))   # Windowing
        
        # Force garbage collection to clean up temporary arrays
        gc.collect()
        
        self.cpu_cache_warmed = True
        LOGGER.warning("CPU caches warmed successfully")
    
    def _initialize_thread_pools(self) -> None:
        """Initialize thread and process pools."""
        LOGGER.warning("Initializing thread pools...")
        
        # Get optimal pool sizes based on system
        cpu_count = multiprocessing.cpu_count()
        optimal_threads = min(cpu_count * 2, 16)  # 2x CPU cores, max 16
        optimal_processes = max(1, cpu_count - 1)  # Leave 1 core for main thread
        
        LOGGER.warning(f"System has {cpu_count} CPU cores")
        LOGGER.warning(f"Initializing {optimal_threads} threads and {optimal_processes} processes")
        
        # Initialize thread pool
        self.thread_pool = ThreadPoolExecutor(
            max_workers=optimal_threads,
            thread_name_prefix="AudioProcessor"
        )
        
        # Initialize process pool (for CPU-intensive tasks like FFT)
        self.process_pool = ProcessPoolExecutor(
            max_workers=optimal_processes,
            mp_context=multiprocessing.get_context('spawn')
        )
        
        # Warm up the pools with dummy tasks
        self._warm_thread_pools()
        
        self.thread_pools_initialized = True
        LOGGER.warning("Thread pools initialized and warmed successfully")
    
    def _warm_thread_pools(self) -> None:
        """Warm up thread pools with dummy tasks."""
        LOGGER.warning("Warming thread pools...")
        
        # Submit dummy tasks to warm up thread pool
        dummy_futures = []
        for i in range(10):
            future = self.thread_pool.submit(self._dummy_task, i)
            dummy_futures.append(future)
        
        # Wait for completion
        for future in dummy_futures:
            future.result()
        
        LOGGER.warning("Thread pools warmed successfully")
    
    def _dummy_task(self, task_id: int) -> str:
        """Dummy task to warm up thread pools."""
        time.sleep(0.01)  # Minimal sleep to ensure thread creation
        return f"Task {task_id} completed"
    
    def _prewarm_audio_processing(self) -> None:
        """Pre-warm audio processing functions."""
        LOGGER.warning("Pre-warming audio processing functions...")
        
        # Import audio processing modules to ensure they're loaded
        try:
            # This will trigger module loading and any initialization
            import librosa
            LOGGER.warning("Librosa module loaded and initialized")
        except ImportError:
            LOGGER.warning("Librosa not available, skipping audio processing pre-warm")
        
        # CRITICAL: Pre-load PYIN engine to avoid 6+ second delays during first audio frame
        LOGGER.warning("Pre-loading PYIN pitch detection engine...")
        try:
            # Import librosa for PYIN pitch detection
            import librosa
            
            # Create dummy audio sample (1 second at 22.05kHz - matching your actual usage)
            dummy_audio = np.random.random(22050).astype(np.float32)
            sample_rate = 22050
            frame_length = 2048
            hop_length = 512
            
            start_time = time.time()
            
            # This is the expensive operation that causes the 6+ second delays
            # Call librosa.pyin directly to pre-load and cache the engine
            f0, voiced_flag, voiced_prob = librosa.pyin(
                y=dummy_audio, sr=sample_rate,
                fmin=180, fmax=4000,
                frame_length=frame_length, hop_length=hop_length, center=True
            )
            
            load_time = time.time() - start_time
            LOGGER.warning(f"PYIN engine pre-loaded successfully in {load_time:.2f} seconds")
            LOGGER.warning(f"Generated {len(f0)} pitch estimates from dummy audio")
            
            # AGGRESSIVE CACHING: Run multiple times with different audio to warm up all internal caches
            LOGGER.warning("Running aggressive PYIN engine warming with multiple audio samples...")
            warming_samples = [
                np.random.random(22050).astype(np.float32),  # Different random audio
                np.random.random(22050).astype(np.float32),  # Another different sample
                np.random.random(22050).astype(np.float32),  # Third sample
            ]
            
            for i, sample in enumerate(warming_samples):
                warm_start = time.time()
                f0_warm, voiced_flag_warm, voiced_prob_warm = librosa.pyin(
                    y=sample, sr=sample_rate,
                    fmin=180, fmax=4000,
                    frame_length=frame_length, hop_length=hop_length, center=True
                )
                warm_time = time.time() - warm_start
                LOGGER.warning(f"Warming sample {i+1}: {warm_time:.3f}s")
            
            # Final verification with original audio (should be fastest now)
            verify_start = time.time()
            f0_verify, voiced_flag_verify, voiced_prob_verify = librosa.pyin(
                y=dummy_audio, sr=sample_rate,
                fmin=180, fmax=4000,
                frame_length=frame_length, hop_length=hop_length, center=True
            )
            verify_time = time.time() - verify_start
            
            LOGGER.warning(f"PYIN engine final verification: {verify_time:.3f}s")
            if verify_time < 0.1:
                LOGGER.warning("✓ PYIN engine is now cached and ready for fast processing")
            elif verify_time < 0.5:
                LOGGER.warning(f"✓ PYIN engine warmed up (acceptable: {verify_time:.3f}s)")
            else:
                LOGGER.warning(f"PYIN engine still slow after warming: {verify_time:.3f}s")
            
        except ImportError as e:
            LOGGER.warning(f"Librosa not available for PYIN pre-loading: {e}")
        except Exception as e:
            LOGGER.error(f"PYIN engine pre-loading failed: {e}")
            # Continue startup even if PYIN pre-loading fails
        
        # Pre-warm audio resampling operations (major bottleneck - 700ms-1.3s delays)
        LOGGER.warning("Pre-warming audio resampling operations...")
        try:
            # Create test audio samples at different sample rates
            test_audio_44100 = np.random.random(44100).astype(np.float32)  # 1 second at 44.1kHz
            test_audio_22050 = np.random.random(22050).astype(np.float32)  # 1 second at 22.05kHz
            test_audio_16000 = np.random.random(16000).astype(np.float32)  # 1 second at 16kHz
            
            # Warm up 44.1kHz → 22.05kHz resampling (major bottleneck)
            LOGGER.warning("Warming up 44.1kHz → 22.05kHz resampling...")
            for i in range(5):
                start_time = time.time()
                resampled = librosa.resample(test_audio_44100, orig_sr=44100, target_sr=22050)
                resample_time = time.time() - start_time
                LOGGER.warning(f"  44.1kHz→22.05kHz warm {i+1}: {resample_time:.3f}s ({len(resampled)} samples)")
            
            # Warm up 22.05kHz → 16kHz resampling (another bottleneck)
            LOGGER.warning("Warming up 22.05kHz → 16kHz resampling...")
            for i in range(5):
                start_time = time.time()
                resampled = librosa.resample(test_audio_22050, orig_sr=22050, target_sr=16000)
                resample_time = time.time() - start_time
                LOGGER.warning(f"  22.05kHz→16kHz warm {i+1}: {resample_time:.3f}s ({len(resampled)} samples)")
            
            # Warm up 44.1kHz → 16kHz resampling (direct conversion)
            LOGGER.warning("Warming up 44.1kHz → 16kHz resampling...")
            for i in range(3):
                start_time = time.time()
                resampled = librosa.resample(test_audio_44100, orig_sr=44100, target_sr=16000)
                resample_time = time.time() - start_time
                LOGGER.warning(f"  44.1kHz→16kHz warm {i+1}: {resample_time:.3f}s ({len(resampled)} samples)")
            
            # Final verification - should be much faster now
            LOGGER.warning("Final verification of resampling performance...")
            verify_start = time.time()
            final_resample = librosa.resample(test_audio_44100, orig_sr=44100, target_sr=22050)
            verify_time = time.time() - verify_start
            
            LOGGER.warning(f"Final 44.1kHz→22.05kHz resampling: {verify_time:.3f}s")
            if verify_time < 0.1:
                LOGGER.warning("✓ Audio resampling is now cached and ready for fast processing")
            elif verify_time < 0.3:
                LOGGER.warning(f"✓ Audio resampling warmed up (acceptable: {verify_time:.3f}s)")
            else:
                LOGGER.warning(f"Audio resampling still slow after warming: {verify_time:.3f}s")
                
        except Exception as e:
            LOGGER.error(f"Audio resampling pre-warming failed: {e}")
            # Continue startup even if resampling pre-warming fails
        
        # Pre-warm numpy operations we'll use frequently
        sample_data = np.random.random(1000).astype(np.float32)
        for _ in range(100):
            # Common audio processing operations
            result = np.fft.fft(sample_data)
            result = np.abs(result)
            result = np.log10(result + 1e-10)  # Avoid log(0)
        
        LOGGER.warning("Audio processing functions pre-warmed successfully")
    
    def get_memory_pool(self, key: str) -> Optional[np.ndarray]:
        """Get a pre-allocated memory buffer."""
        if key in self.memory_pools and self.memory_pools[key]:
            return self.memory_pools[key].pop()
        return None
    
    def return_memory_pool(self, key: str, buffer: np.ndarray) -> None:
        """Return a memory buffer to the pool."""
        if key in self.memory_pools:
            # Clear the buffer before returning
            buffer.fill(0)
            self.memory_pools[key].append(buffer)
    
    def get_thread_pool(self) -> ThreadPoolExecutor:
        """Get the thread pool executor."""
        if not self.thread_pool:
            raise RuntimeError("Thread pool not initialized")
        return self.thread_pool
    
    def get_process_pool(self) -> ProcessPoolExecutor:
        """Get the process pool executor."""
        if not self.process_pool:
            raise RuntimeError("Process pool not initialized")
        return self.process_pool
    
    def cleanup(self) -> None:
        """Clean up resources."""
        LOGGER.warning("Cleaning up resource pre-warmer...")
        
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        
        if self.process_pool:
            self.process_pool.shutdown(wait=True)
        
        # Clear memory pools
        self.memory_pools.clear()
        
        LOGGER.warning("Resource cleanup completed")

# Global instance
RESOURCE_PREWARMER = ResourcePrewarmer()

def prewarm_resources() -> None:
    """Global function to pre-warm all resources."""
    RESOURCE_PREWARMER.prewarm_all_resources()

def cleanup_resources() -> None:
    """Global function to cleanup resources."""
    RESOURCE_PREWARMER.cleanup()

def prewarm_pyin_engine() -> bool:
    """Specifically pre-warm the PYIN engine to avoid 6+ second delays during first audio frame."""
    try:
        import librosa
        import numpy as np
        import time
        
        LOGGER.warning("Pre-loading PYIN pitch detection engine...")
        
        # Create dummy audio sample (1 second at 22.05kHz - matching actual usage)
        dummy_audio = np.random.random(22050).astype(np.float32)
        sample_rate = 22050
        frame_length = 2048
        hop_length = 512
        
        start_time = time.time()
        
        # Call librosa.pyin directly to pre-load and cache the engine
        f0, voiced_flag, voiced_prob = librosa.pyin(
            y=dummy_audio, sr=sample_rate,
            fmin=180, fmax=4000,
            frame_length=frame_length, hop_length=hop_length, center=True
        )
        
        load_time = time.time() - start_time
        LOGGER.warning(f"PYIN engine pre-loaded successfully in {load_time:.2f} seconds")
        LOGGER.warning(f"Generated {len(f0)} pitch estimates from dummy audio")
        
        # AGGRESSIVE CACHING: Run multiple times with different audio to warm up all internal caches
        LOGGER.warning("Running aggressive PYIN engine warming with multiple audio samples...")
        warming_samples = [
            np.random.random(22050).astype(np.float32),  # Different random audio
            np.random.random(22050).astype(np.float32),  # Another different sample
            np.random.random(22050).astype(np.float32),  # Third sample
        ]
        
        for i, sample in enumerate(warming_samples):
            warm_start = time.time()
            f0_warm, voiced_flag_warm, voiced_prob_warm = librosa.pyin(
                y=sample, sr=sample_rate,
                fmin=180, fmax=4000,
                frame_length=frame_length, hop_length=hop_length, center=True
            )
            warm_time = time.time() - warm_start
            LOGGER.warning(f"Warming sample {i+1}: {warm_time:.3f}s")
        
        # Final verification with original audio (should be fastest now)
        verify_start = time.time()
        f0_verify, voiced_flag_verify, voiced_prob_verify = librosa.pyin(
            y=dummy_audio, sr=sample_rate,
            fmin=180, fmax=4000,
            frame_length=frame_length, hop_length=hop_length, center=True
        )
        verify_time = time.time() - verify_start
        
        LOGGER.warning(f"PYIN engine final verification: {verify_time:.3f}s")
        if verify_time < 0.1:
            LOGGER.warning("✓ PYIN engine is now cached and ready for fast processing")
        elif verify_time < 0.5:
            LOGGER.warning(f"✓ PYIN engine warmed up (acceptable: {verify_time:.3f}s)")
        else:
            LOGGER.warning(f"PYIN engine still slow after warming: {verify_time:.3f}s")
        
        return True
        
    except ImportError as e:
        LOGGER.warning(f"Librosa not available for PYIN pre-loading: {e}")
        return False
    except Exception as e:
        LOGGER.error(f"PYIN engine pre-loading failed: {e}")
        return False

def prewarm_audio_resampling() -> bool:
    """Specifically pre-warm audio resampling operations to avoid 700ms-1.3s delays."""
    try:
        import librosa
        import numpy as np
        import time
        
        LOGGER.warning("Pre-warming audio resampling operations...")
        
        # Create test audio samples at different sample rates
        test_audio_44100 = np.random.random(44100).astype(np.float32)  # 1 second at 44.1kHz
        test_audio_22050 = np.random.random(22050).astype(np.float32)  # 1 second at 22.05kHz
        test_audio_16000 = np.random.random(16000).astype(np.float32)  # 1 second at 16kHz
        
        # Warm up 44.1kHz → 22.05kHz resampling (major bottleneck)
        LOGGER.warning("Warming up 44.1kHz → 22.05kHz resampling...")
        for i in range(5):
            start_time = time.time()
            resampled = librosa.resample(test_audio_44100, orig_sr=44100, target_sr=22050)
            resample_time = time.time() - start_time
            LOGGER.warning(f"  44.1kHz→22.05kHz warm {i+1}: {resample_time:.3f}s ({len(resampled)} samples)")
        
        # Warm up 22.05kHz → 16kHz resampling (another bottleneck)
        LOGGER.warning("Warming up 22.05kHz → 16kHz resampling...")
        for i in range(5):
            start_time = time.time()
            resampled = librosa.resample(test_audio_22050, orig_sr=22050, target_sr=16000)
            resample_time = time.time() - start_time
            LOGGER.warning(f"  22.05kHz→16kHz warm {i+1}: {resample_time:.3f}s ({len(resampled)} samples)")
        
        # Warm up 44.1kHz → 16kHz resampling (direct conversion)
        LOGGER.warning("Warming up 44.1kHz → 16kHz resampling...")
        for i in range(3):
            start_time = time.time()
            resampled = librosa.resample(test_audio_44100, orig_sr=44100, target_sr=16000)
            resample_time = time.time() - start_time
            LOGGER.warning(f"  44.1kHz→16kHz warm {i+1}: {resample_time:.3f}s ({len(resampled)} samples)")
        
        # Final verification - should be much faster now
        LOGGER.warning("Final verification of resampling performance...")
        verify_start = time.time()
        final_resample = librosa.resample(test_audio_44100, orig_sr=44100, target_sr=22050)
        verify_time = time.time() - verify_start
        
        LOGGER.warning(f"Final 44.1kHz→22.05kHz resampling: {verify_time:.3f}s")
        if verify_time < 0.1:
            LOGGER.warning("✓ Audio resampling is now cached and ready for fast processing")
        elif verify_time < 0.3:
            LOGGER.warning(f"✓ Audio resampling warmed up (acceptable: {verify_time:.3f}s)")
        else:
            LOGGER.warning(f"Audio resampling still slow after warming: {verify_time:.3f}s")
        
        return True
        
    except ImportError as e:
        LOGGER.warning(f"Librosa not available for audio resampling pre-warming: {e}")
        return False
    except Exception as e:
        LOGGER.error(f"Audio resampling pre-warming failed: {e}")
        return False

if __name__ == "__main__":
    # Test the pre-warmer
    logging.basicConfig(level=logging.INFO)
    
    try:
        prewarm_resources()
        print("Resource pre-warming test completed successfully")
    finally:
        cleanup_resources()
