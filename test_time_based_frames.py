#!/usr/bin/env python3
"""
Simple test to verify time-based frame numbering.
This simulates the exact scenario: client sends 681ms + 684ms = 1365ms total
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from SERVER_ENGINE_AUDIO_FRAME_ALIGNMENT import AudioFrameAlignmentBuffer
from SERVER_ENGINE_APP_VARIABLES import AUDIO_BYTES_PER_FRAME, AUDIO_SAMPLE_RATE, AUDIO_FRAME_MS

def test_time_based_frames():
    """Test time-based frame numbering with realistic client data."""
    
    print("Testing Time-Based Frame Numbering")
    print("=" * 50)
    print(f"Target frame size: {AUDIO_FRAME_MS}ms")
    print(f"Target sample rate: {AUDIO_SAMPLE_RATE}Hz")
    print(f"Target bytes per frame: {AUDIO_BYTES_PER_FRAME}")
    print()
    
    # Create a test buffer
    buffer = AudioFrameAlignmentBuffer(recording_id=999)
    
    # Simulate client sending 681ms chunk (first chunk)
    print("Test: Client sends 681ms chunk")
    samples_681ms = int(681 * AUDIO_SAMPLE_RATE / 1000)  # 681ms worth of samples
    bytes_681ms = samples_681ms * 2  # PCM16 = 2 bytes per sample
    chunk1 = b'\x00\x00' * samples_681ms
    
    print(f"  Chunk 1: {bytes_681ms} bytes ({samples_681ms} samples, {681}ms)")
    
    frames1 = buffer.add_audio_chunk(chunk1, client_frame_no=1)
    print(f"  Frames produced: {len(frames1)}")
    for frame_no, frame_bytes in frames1:
        start_ms = (frame_no - 1) * 100
        end_ms = (frame_no * 100) - 1
        print(f"    Frame {frame_no}: {start_ms}-{end_ms}ms ({len(frame_bytes)} bytes)")
    
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Simulate client sending 684ms chunk (second chunk)
    print("Test: Client sends 684ms chunk")
    samples_684ms = int(684 * AUDIO_SAMPLE_RATE / 1000)  # 684ms worth of samples
    bytes_684ms = samples_684ms * 2  # PCM16 = 2 bytes per sample
    chunk2 = b'\x00\x00' * samples_684ms
    
    print(f"  Chunk 2: {bytes_684ms} bytes ({samples_684ms} samples, {684}ms)")
    
    frames2 = buffer.add_audio_chunk(chunk2, client_frame_no=2)
    print(f"  Frames produced: {len(frames2)}")
    for frame_no, frame_bytes in frames2:
        start_ms = (frame_no - 1) * 100
        end_ms = (frame_no * 100) - 1
        print(f"    Frame {frame_no}: {start_ms}-{end_ms}ms ({len(frame_bytes)} bytes)")
    
    print(f"  Buffer status: {buffer.get_buffer_status()}")
    print()
    
    # Flush remaining audio
    print("Test: Flush remaining audio")
    remaining_frames = buffer.flush_remaining_audio()
    print(f"  Remaining frames: {len(remaining_frames)}")
    for frame_no, frame_bytes in remaining_frames:
        start_ms = (frame_no - 1) * 100
        end_ms = (frame_no * 100) - 1
        print(f"    Frame {frame_no}: {start_ms}-{end_ms}ms ({len(frame_bytes)} bytes)")
        print(f"      Note: This frame may be shorter than 100ms")
    
    print()
    
    # Summary
    print("Test Summary")
    print("=" * 50)
    total_ms = (bytes_681ms + bytes_684ms) // 2 * 1000 // AUDIO_SAMPLE_RATE
    expected_frames = (total_ms + 99) // 100  # Round up to include partial frames
    
    print(f"Total audio received: {total_ms}ms")
    print(f"Expected frames: {expected_frames}")
    print(f"Actual frames produced: {buffer.total_frames_produced}")
    print(f"Total bytes received: {buffer.total_bytes_received}")
    print(f"Total bytes in frames: {buffer.total_frames_produced * AUDIO_BYTES_PER_FRAME}")
    
    # Verify frame numbering
    print("\nFrame Numbering Verification:")
    print("Frame 1: 0-99ms")
    print("Frame 2: 100-199ms")
    print("Frame 3: 200-299ms")
    print("...")
    print(f"Frame {buffer.total_frames_produced}: {((buffer.total_frames_produced - 1) * 100)}-{((buffer.total_frames_produced * 100) - 1)}ms")

if __name__ == "__main__":
    test_time_based_frames()
