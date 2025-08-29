#!/usr/bin/env python3
"""
Test script for the audio processing pool to verify parallel processing is working.
"""

import numpy as np
import time
from SERVER_ENGINE_AUDIO_PROCESSING_POOL import (
    pyin_relative_rows_parallel, 
    resample_parallel, 
    wait_for_futures,
    get_pool_status
)

def test_parallel_processing():
    """Test that parallel processing is working correctly."""
    
    print("🎵 Testing Audio Processing Pool...")
    
    # Check pool status
    status = get_pool_status()
    print(f"Pool Status: {status}")
    
    # Create test audio data (1 second of 22050 Hz sine wave)
    sample_rate = 22050
    duration = 1.0  # 1 second
    t = np.linspace(0, duration, int(sample_rate * duration), False)
    test_audio = np.sin(2 * np.pi * 440 * t).astype(np.float32)  # 440 Hz A note
    
    print(f"Test audio: {len(test_audio)} samples at {sample_rate} Hz")
    
    # Test parallel resampling
    print("\n🔄 Testing parallel resampling...")
    start_time = time.time()
    
    # Submit multiple resampling tasks
    futures = []
    for i in range(3):
        future = resample_parallel(test_audio, 22050, 16000)
        futures.append(future)
        print(f"  Submitted resampling task {i+1}")
    
    # Wait for all to complete
    results = wait_for_futures(*futures)
    end_time = time.time()
    
    print(f"  All resampling completed in {end_time - start_time:.3f} seconds")
    print(f"  Results: {[len(r) if r is not None else 'None' for r in results]}")
    
    # Test parallel PYIN
    print("\n🎼 Testing parallel PYIN...")
    start_time = time.time()
    
    # Submit multiple PYIN tasks
    futures = []
    for i in range(3):
        future = pyin_relative_rows_parallel(test_audio, 22050)
        futures.append(future)
        print(f"  Submitted PYIN task {i+1}")
    
    # Wait for all to complete
    results = wait_for_futures(*futures)
    end_time = time.time()
    
    print(f"  All PYIN completed in {end_time - start_time:.3f} seconds")
    print(f"  Results: {[len(r) if r is not None else 'None' for r in results]}")
    
    print("\n✅ Audio processing pool test completed!")

if __name__ == "__main__":
    test_parallel_processing()
