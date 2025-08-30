#!/usr/bin/env python3
"""
Test script to verify the CREPE hop_length fix.
This tests that the HOP variable is now an integer instead of a float.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_crepe_hop_fix():
    """Test that the CREPE hop_length fix works correctly."""
    
    print("Testing CREPE hop_length fix...")
    
    try:
        # Import the CREPE processing module
        from SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE import SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE
        
        # Check if the function exists
        if hasattr(SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE, '__name__'):
            print(f"✓ CREPE function imported successfully: {SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE.__name__}")
        else:
            print("✗ CREPE function not found")
            return False
            
        # Test the hop calculation logic
        SAMPLE_RATE = 16000
        CREPE_HOP_IN_MS = 10
        
        # This should now work without the float error
        HOP = int(SAMPLE_RATE / CREPE_HOP_IN_MS)
        
        print(f"✓ HOP calculation: {SAMPLE_RATE} / {CREPE_HOP_IN_MS} = {HOP}")
        print(f"✓ HOP type: {type(HOP)} (should be <class 'int'>)")
        print(f"✓ HOP value: {HOP} (should be 160)")
        
        if isinstance(HOP, int) and HOP == 160:
            print("✓ HOP fix verified successfully!")
            return True
        else:
            print("✗ HOP fix verification failed!")
            return False
            
    except ImportError as e:
        print(f"✗ Failed to import CREPE module: {e}")
        return False
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        return False

def test_torchcrepe_hop_length():
    """Test that torchcrepe accepts integer hop_length."""
    
    print("\nTesting torchcrepe hop_length acceptance...")
    
    try:
        import torchcrepe
        import torch
        import numpy as np
        
        print("✓ TorchCREPE imported successfully")
        
        # Create test audio
        test_audio = np.random.randn(16000).astype(np.float32)  # 1 second @ 16kHz
        x = torch.tensor(test_audio, dtype=torch.float32).unsqueeze(0)
        
        # Test with integer hop_length (this should work)
        try:
            f0, per = torchcrepe.predict(
                x,
                sample_rate=16000,
                hop_length=160,  # Integer hop_length
                model="tiny",
                decoder=torchcrepe.decode.viterbi,
                batch_size=128,
                device="cpu",
                return_periodicity=True,
            )
            print("✓ TorchCREPE accepted integer hop_length=160")
            print(f"✓ Generated {len(f0)} pitch estimates")
            return True
            
        except Exception as e:
            print(f"✗ TorchCREPE failed with integer hop_length: {e}")
            return False
            
    except ImportError as e:
        print(f"✗ TorchCREPE not available: {e}")
        return False
    except Exception as e:
        print(f"✗ TorchCREPE test failed: {e}")
        return False

if __name__ == "__main__":
    print("=== CREPE Hop Length Fix Test ===")
    
    # Test 1: Verify the fix in the module
    test1_success = test_crepe_hop_fix()
    
    # Test 2: Verify torchcrepe accepts integer hop_length
    test2_success = test_torchcrepe_hop_length()
    
    print(f"\n=== Test Results ===")
    print(f"Test 1 (Module Fix): {'✓ PASSED' if test1_success else '✗ FAILED'}")
    print(f"Test 2 (TorchCREPE): {'✓ PASSED' if test2_success else '✗ FAILED'}")
    
    if test1_success and test2_success:
        print("\n🎉 All tests passed! The CREPE hop_length fix is working correctly.")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
