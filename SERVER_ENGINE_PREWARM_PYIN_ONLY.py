#!/usr/bin/env python3
"""
Standalone PYIN Engine Pre-warming Script

This script specifically pre-loads the PYIN pitch detection engine to avoid
the 6+ second delays that occur during the first audio frame processing.

Usage:
    python SERVER_ENGINE_PREWARM_PYIN_ONLY.py
"""

import time
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

def prewarm_pyin_engine() -> bool:
    """Pre-load the PYIN engine to avoid 6+ second delays during first audio frame."""
    try:
        LOGGER.info("Starting PYIN engine pre-warming...")
        
        # Import librosa for PYIN pitch detection
        import librosa
        
        # Create dummy audio sample (1 second at 22.05kHz - matching actual usage)
        LOGGER.info("Creating dummy audio sample for PYIN engine initialization...")
        dummy_audio = np.random.random(22050).astype(np.float32)
        sample_rate = 22050
        frame_length = 2048
        hop_length = 512
        
        # This is the expensive operation that causes the 6+ second delays
        LOGGER.info("Loading PYIN engine with dummy audio sample...")
        start_time = time.time()
        
        # Call librosa.pyin directly to pre-load and cache the engine
        f0, voiced_flag, voiced_prob = librosa.pyin(
            y=dummy_audio, sr=sample_rate,
            fmin=180, fmax=4000,
            frame_length=frame_length, hop_length=hop_length, center=True
        )
        
        load_time = time.time() - start_time
        LOGGER.info(f"✓ PYIN engine pre-loaded successfully in {load_time:.2f} seconds")
        LOGGER.info(f"Generated {len(f0)} pitch estimates from dummy audio")
        
        # AGGRESSIVE CACHING: Run multiple times with different audio to warm up all internal caches
        LOGGER.info("Running aggressive PYIN engine warming with multiple audio samples...")
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
            LOGGER.info(f"Warming sample {i+1}: {warm_time:.3f}s")
        
        # Final verification with original audio (should be fastest now)
        LOGGER.info("Final verification of PYIN engine...")
        verify_start = time.time()
        f0_verify, voiced_flag_verify, voiced_prob_verify = librosa.pyin(
            y=dummy_audio, sr=sample_rate,
            fmin=180, fmax=4000,
            frame_length=frame_length, hop_length=hop_length, center=True
        )
        verify_time = time.time() - verify_start
        
        LOGGER.info(f"✓ PYIN engine final verification: {verify_time:.3f}s")
        LOGGER.info(f"✓ First load: {load_time:.2f}s, Final verification: {verify_time:.3f}s")
        
        if verify_time < 0.1:  # Should be very fast on subsequent calls
            LOGGER.info("✓ PYIN engine is now cached and ready for fast processing")
        elif verify_time < 0.5:
            LOGGER.info(f"✓ PYIN engine warmed up (acceptable: {verify_time:.3f}s)")
        else:
            LOGGER.warning(f"PYIN engine still slow after warming: {verify_time:.3f}s")
        
        return True
        
    except ImportError as e:
        LOGGER.error(f"❌ PYIN engine module not available: {e}")
        LOGGER.error("Make sure SERVER_ENGINE_PYIN_ENGINE_LOAD_HZ_INS.py exists")
        return False
        
    except Exception as e:
        LOGGER.error(f"❌ PYIN engine pre-loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function to pre-warm PYIN engine."""
    print("=" * 60)
    print("PYIN ENGINE PRE-WARMING SCRIPT")
    print("=" * 60)
    print("This script pre-loads the PYIN pitch detection engine to avoid")
    print("6+ second delays during the first audio frame processing.")
    print()
    
    success = prewarm_pyin_engine()
    
    print()
    if success:
        print("✓ PYIN engine pre-warming completed successfully!")
        print("  The engine is now cached and ready for fast processing.")
        print("  Subsequent audio frames should process without delays.")
    else:
        print("❌ PYIN engine pre-warming failed!")
        print("  Check the error messages above for details.")
        print("  Audio processing may still experience delays.")
    
    print()
    print("=" * 60)

if __name__ == "__main__":
    main()
