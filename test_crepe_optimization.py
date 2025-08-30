#!/usr/bin/env python3
"""
Test script to verify CREPE optimization in the prewarming module.
This will help diagnose why CREPE is still slow after warming.
"""

import time
import logging
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

def test_crepe_optimization():
    """Test the CREPE optimization with different configurations."""
    
    LOGGER.info("Testing CREPE optimization...")
    
    try:
        # Import the prewarming module
        from SERVER_ENGINE_PREWARM_RESOURCES import prewarm_crepe_engine
        
        # Test CREPE prewarming
        LOGGER.info("Starting CREPE prewarming test...")
        start_time = time.time()
        
        success = prewarm_crepe_engine()
        
        total_time = time.time() - start_time
        LOGGER.info(f"CREPE prewarming test completed in {total_time:.2f} seconds")
        LOGGER.info(f"Success: {success}")
        
        if success:
            LOGGER.info("✓ CREPE optimization test passed!")
        else:
            LOGGER.error("✗ CREPE optimization test failed!")
            
    except ImportError as e:
        LOGGER.error(f"Failed to import prewarming module: {e}")
        return False
    except Exception as e:
        LOGGER.error(f"CREPE optimization test failed: {e}")
        return False
    
    return True

def test_crepe_direct():
    """Test CREPE directly to see baseline performance."""
    
    LOGGER.info("Testing CREPE directly...")
    
    try:
        import torchcrepe
        import torch
        import numpy as np
        
        # Check device
        device = "cuda" if torch.cuda.is_available() else "cpu"
        LOGGER.info(f"Using device: {device}")
        
        # Test different model sizes
        model_sizes = ["tiny", "small", "medium", "full"]
        batch_sizes = [64, 128, 256, 512, 1024]
        
        # Generate test audio
        test_audio = np.random.randn(16000).astype(np.float32)  # 1 second @ 16kHz
        x = torch.tensor(test_audio, dtype=torch.float32, device=device).unsqueeze(0)
        
        LOGGER.info("Testing different model configurations...")
        
        for model_size in model_sizes:
            for batch_size in batch_sizes:
                try:
                    start_time = time.time()
                    
                    f0, per = torchcrepe.predict(
                        x,
                        sample_rate=16000,
                        hop_length=160,
                        model=model_size,
                        decoder=torchcrepe.decode.viterbi,
                        batch_size=batch_size,
                        device=device,
                        return_periodicity=True,
                    )
                    
                    inference_time = time.time() - start_time
                    LOGGER.info(f"  {model_size} model, batch {batch_size}: {inference_time:.3f}s")
                    
                    # Clear memory
                    del f0, per
                    if device == "cuda":
                        torch.cuda.empty_cache()
                    
                except Exception as e:
                    LOGGER.warning(f"  {model_size} model, batch {batch_size}: FAILED - {e}")
                    continue
        
        LOGGER.info("Direct CREPE testing completed")
        
    except ImportError as e:
        LOGGER.error(f"TorchCREPE not available: {e}")
        return False
    except Exception as e:
        LOGGER.error(f"Direct CREPE test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    LOGGER.info("=== CREPE Optimization Test ===")
    
    # Test 1: Test the prewarming module
    LOGGER.info("\n--- Test 1: CREPE Prewarming Module ---")
    test_crepe_optimization()
    
    # Test 2: Test CREPE directly
    LOGGER.info("\n--- Test 2: Direct CREPE Testing ---")
    test_crepe_direct()
    
    LOGGER.info("\n=== Test Completed ===")
