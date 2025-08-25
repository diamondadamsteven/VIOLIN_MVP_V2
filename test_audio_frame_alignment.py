#!/usr/bin/env python3
"""
Test script for the audio frame alignment system.
This script simulates receiving variable-length audio chunks and verifies
that the system produces correctly aligned 100ms frames.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from SERVER_ENGINE_AUDIO_FRAME_ALIGNMENT import AudioFrameAlignmentBuffer, validate_frame_bytes
from SERVER_ENGINE_APP_VARIABLES import AUDIO_BYTES_PER_FRAME, AUDIO_SAMPLE_RATE, AUDIO_FRAME_MS

def test_audio_frame_alignment():
    """Test the audio frame alignment system with various chunk sizes."""
    
    print("Testing Audio Frame Alignment System")
    print("=" * 50)
    print(f"Target frame size: {AUDIO_FRAME_MS}ms")
    print(f"Target sample rate: {AUDIO_SAMPLE_RATE}Hz")
    print(f"Target bytes per frame: {AUDIO_BYTES_PER_FRAME}")
    print(f"Target samples per frame: {AUDIO_BYTES_PER_FRAME // 2}")
    print()
    
    # Create a test buffer
    buffer = AudioFrameAlignmentBuffer(recording_id=999)
    
    # Test case 1: Send exactly one frame worth of data
    print("Test 1: Send exactly one frame (8820 bytes)")
    chunk1 = b'\x00\x00' * (AUDIO_BYTES_PER_FRAME // 2)  # 8820 bytes of silence
    frames1 = buffer.add_audio_chunk(chunk1, client_frame_no=1)
    print(f"  Frames produced: {len(frames1)}")
    for frame_no, frame_bytes in frames1:
        print(f"  Frame {frame_no}: {len(frame_bytes)} bytes")
        print(f"    Valid: {validate_frame_bytes(frame_bytes, frame_no)}")
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Test case 2: Send a chunk that's less than one frame
    print("Test 2: Send partial frame (4000 bytes)")
    chunk2 = b'\x00\x00' * 2000  # 4000 bytes of silence
    frames2 = buffer.add_audio_chunk(chunk2, client_frame_no=2)
    print(f"  Frames produced: {len(frames2)}")
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Test case 3: Send another chunk that completes the frame
    print("Test 3: Send remaining data to complete frame")
    chunk3 = b'\x00\x00' * 2410  # 4820 bytes to complete the frame
    frames3 = buffer.add_audio_chunk(chunk3, client_frame_no=3)
    print(f"  Frames produced: {len(frames3)}")
    for frame_no, frame_bytes in frames3:
        print(f"  Frame {frame_no}: {len(frame_bytes)} bytes")
        print(f"    Valid: {validate_frame_bytes(frame_bytes, frame_no)}")
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Test case 4: Send multiple frames worth of data
    print("Test 4: Send multiple frames worth of data (20000 bytes)")
    chunk4 = b'\x00\x00' * 10000  # 20000 bytes of silence
    frames4 = buffer.add_audio_chunk(chunk4, client_frame_no=4)
    print(f"  Frames produced: {len(frames4)}")
    for frame_no, frame_bytes in frames4:
        print(f"  Frame {frame_no}: {len(frame_bytes)} bytes")
        print(f"    Valid: {validate_frame_bytes(frame_bytes, frame_no)}")
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Test case 5: Send odd-length chunk (simulating the problem we're fixing)
    print("Test 5: Send odd-length chunk (simulating client timing issues)")
    chunk5 = b'\x00\x00' * 3000 + b'\x00'  # 6001 bytes (odd length)
    frames5 = buffer.add_audio_chunk(chunk5, client_frame_no=5)
    print(f"  Frames produced: {len(frames5)}")
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Test case 6: Flush remaining audio
    print("Test 6: Flush remaining audio")
    remaining_frames = buffer.flush_remaining_audio()
    print(f"  Remaining frames: {len(remaining_frames)}")
    for frame_no, frame_bytes in remaining_frames:
        print(f"  Frame {frame_no}: {len(frame_bytes)} bytes")
        print(f"    Note: This frame may be shorter than 100ms")
    print()
    
    # Summary
    print("Test Summary")
    print("=" * 50)
    print(f"Total frames produced: {buffer.total_frames_produced}")
    print(f"Total bytes received: {buffer.total_bytes_received}")
    print(f"Total bytes in frames: {buffer.total_frames_produced * AUDIO_BYTES_PER_FRAME}")
    print(f"Buffer efficiency: {(buffer.total_frames_produced * AUDIO_BYTES_PER_FRAME / buffer.total_bytes_received) * 100:.1f}%")

if __name__ == "__main__":
    test_audio_frame_alignment()
