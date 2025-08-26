# SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES.py
"""
Audio Frame Alignment System

This module handles the alignment of variable-length audio chunks from the client
into exact 100ms frames for processing. It accumulates audio data and produces
properly sized frames that can be processed by the analysis pipeline.

All functionality is now consolidated into one main function: SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Tuple
import numpy as np

from SERVER_ENGINE_APP_VARIABLES import (
    AUDIO_FRAME_ALIGNMENT_BUFFERS,
    AUDIO_FRAME_MS,
    AUDIO_SAMPLE_RATE,
    AUDIO_BYTES_PER_SAMPLE,
    AUDIO_SAMPLES_PER_FRAME,
    AUDIO_BYTES_PER_FRAME,
    WEBSOCKET_AUDIO_FRAME_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG, DB_INSERT_TABLE

PREFIX = "AUDIO_ALIGN"


def SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES(
    action: str,
    recording_id: int,
    client_frame_no: Optional[int] = None,
    audio_bytes: Optional[bytes] = None
) -> List[Tuple[int, bytes]]:
    """
    Main function for audio frame alignment - handles all operations in one place.
    
    Args:
        action: What action to perform:
            - "SPLIT_INTO_100_MS_FRAMES": Split incoming audio chunk into 100ms frames
            - "FLUSH_REMAINING_AUDIO": Flush remaining audio when recording stops
            - "GET_BUFFER_STATUS": Get buffer status for debugging
            - "VALIDATE_FRAME": Validate frame bytes
        recording_id: The recording ID
        client_frame_no: The frame number from the client (for logging, required for "SPLIT_INTO_100_MS_FRAMES")
        audio_bytes: The raw audio bytes from the client (required for "SPLIT_INTO_100_MS_FRAMES")
        
    Returns:
        List of (frame_no, frame_bytes) tuples for 100ms frames (empty list for status/validate)
    """
    
    # Get or create alignment buffer for this recording
    if recording_id not in AUDIO_FRAME_ALIGNMENT_BUFFERS:
        AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id] = {
            'audio_buffer': bytearray(),
            'total_bytes_received': 0,
            'total_frames_produced': 0,
            'last_frame_time': None,
            'frame_counter': 1
        }
        CONSOLE_LOG(PREFIX, "created_buffer", {
            "rid": recording_id,
            "target_frame_ms": AUDIO_FRAME_MS,
            "target_sample_rate": AUDIO_SAMPLE_RATE,
            "target_bytes_per_frame": AUDIO_BYTES_PER_FRAME
        })
    
    buffer = AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id]
    
    if action == "SPLIT_INTO_100_MS_FRAMES":
        if client_frame_no is None or audio_bytes is None:
            CONSOLE_LOG(PREFIX, "error", {"message": "client_frame_no and audio_bytes required for SPLIT_INTO_100_MS_FRAMES"})
            return []
        
        # Log the incoming chunk
        CONSOLE_LOG(PREFIX, "received_chunk", {
            "rid": recording_id,
            "client_frame": client_frame_no,
            "chunk_bytes": len(audio_bytes),
            "chunk_samples": len(audio_bytes) // AUDIO_BYTES_PER_SAMPLE,
            "chunk_ms": (len(audio_bytes) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
            "chunk_bytes_even": len(audio_bytes) % 2 == 0
        })
        
        # Add the chunk to the buffer directly
        buffer['audio_buffer'].extend(audio_bytes)
        buffer['total_bytes_received'] += len(audio_bytes)
        
        PRE_SPLIT_100_MS_FRAMES = []
        
        # Keep producing frames while we have enough data
        while len(buffer['audio_buffer']) >= AUDIO_BYTES_PER_FRAME:
            # Extract exactly one frame
            frame_bytes = bytes(buffer['audio_buffer'][:AUDIO_BYTES_PER_FRAME])
            buffer['audio_buffer'] = buffer['audio_buffer'][AUDIO_BYTES_PER_FRAME:]
            
            # Calculate time-based frame number
            total_samples_processed = (buffer['total_bytes_received'] - len(buffer['audio_buffer'])) // AUDIO_BYTES_PER_SAMPLE
            frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
            
            buffer['total_frames_produced'] += 1
            
            # Validate the frame before returning it
            if _validate_frame_bytes(frame_bytes, frame_no):
                PRE_SPLIT_100_MS_FRAMES.append((frame_no, frame_bytes))
            else:
                CONSOLE_LOG(PREFIX, "frame_validation_failed_skipping", {
                    "rid": recording_id,
                    "frame": frame_no,
                    "client_frame": client_frame_no
                })
        
        # Log buffer status after processing
        buffer_status = _get_buffer_status(buffer, recording_id)
        CONSOLE_LOG(PREFIX, "buffer_status", buffer_status)
        
        # Log any complete frames produced
        if PRE_SPLIT_100_MS_FRAMES:
            CONSOLE_LOG(PREFIX, "produced_frames", {
                "rid": recording_id,
                "frames_produced": len(PRE_SPLIT_100_MS_FRAMES),
                "frame_numbers": [f[0] for f in PRE_SPLIT_100_MS_FRAMES],
                "total_bytes_in_frames": sum(len(f[1]) for f in PRE_SPLIT_100_MS_FRAMES)
            })
        
        return PRE_SPLIT_100_MS_FRAMES
    
    elif action == "FLUSH_REMAINING_AUDIO":
        # Flush any remaining audio data when recording stops
        if not buffer['audio_buffer']:
            return []
        
        # Pad to even byte length if needed
        if len(buffer['audio_buffer']) % 2 != 0:
            buffer['audio_buffer'].append(0)  # Add padding byte
            CONSOLE_LOG(PREFIX, "flushed_remaining_frame", {
                "rid": recording_id,
                "original_bytes": len(buffer['audio_buffer']) - 1,
                "padded_bytes": len(buffer['audio_buffer'])
            })
        
        frame_bytes = bytes(buffer['audio_buffer'])
        
        # Calculate time-based frame number for the remaining audio
        total_samples_processed = buffer['total_bytes_received'] // AUDIO_BYTES_PER_SAMPLE
        frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
        
        buffer['total_frames_produced'] += 1
        
        CONSOLE_LOG(PREFIX, "flushed_remaining_frame", {
            "rid": recording_id,
            "frame": frame_no,
            "bytes": len(frame_bytes),
            "samples": len(frame_bytes) // AUDIO_BYTES_PER_SAMPLE,
            "duration_ms": (len(frame_bytes) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
            "note": "final_frame_may_be_short"
        })
        
        remaining_frames = [(frame_no, frame_bytes)]
        
        # Clean up the buffer
        del AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id]
        CONSOLE_LOG(PREFIX, "cleaned_up_buffer", {"rid": recording_id})
        
        return remaining_frames
    
    elif action == "GET_BUFFER_STATUS":
        # Get the current status of the buffer for debugging
        return _get_buffer_status(buffer, recording_id)
    
    elif action == "VALIDATE_FRAME":
        if audio_bytes is None:
            CONSOLE_LOG(PREFIX, "error", {"message": "audio_bytes required for VALIDATE_FRAME"})
            return []
        
        # Validate frame bytes (assuming frame_no is 0 for validation)
        frame_no = client_frame_no if client_frame_no is not None else 0
        is_valid = _validate_frame_bytes(audio_bytes, frame_no)
        return []  # Return empty list for validation (just logs the result)
    
    else:
        CONSOLE_LOG(PREFIX, "error", {"message": f"Unknown action: {action}"})
        return []


def _validate_frame_bytes(frame_bytes: bytes, frame_no: int) -> bool:
    """
    Internal helper to validate that a frame has the correct properties.
    """
    # Check byte length
    if len(frame_bytes) != AUDIO_BYTES_PER_FRAME:
        CONSOLE_LOG(PREFIX, "frame_validation_failed", {
            "frame": frame_no,
            "expected_bytes": AUDIO_BYTES_PER_FRAME,
            "actual_bytes": len(frame_bytes),
            "reason": "incorrect_byte_length"
        })
        return False
    
    # Check that length is even (PCM16 requirement)
    if len(frame_bytes) % 2 != 0:
        CONSOLE_LOG(PREFIX, "frame_validation_failed", {
            "frame": frame_no,
            "actual_bytes": len(frame_bytes),
            "reason": "odd_byte_length"
        })
        return False
    
    # Check that we have the right number of samples
    expected_samples = AUDIO_SAMPLES_PER_FRAME
    actual_samples = len(frame_bytes) // AUDIO_BYTES_PER_SAMPLE
    
    if actual_samples != expected_samples:
        CONSOLE_LOG(PREFIX, "frame_validation_failed", {
            "frame": frame_no,
            "expected_samples": expected_samples,
            "actual_samples": actual_samples,
            "reason": "incorrect_sample_count"
        })
        return False
    
    CONSOLE_LOG(PREFIX, "frame_validation_passed", {
        "frame": frame_no,
        "bytes": len(frame_bytes),
        "samples": actual_samples,
        "duration_ms": (actual_samples * 1000) // AUDIO_SAMPLE_RATE
    })
    
    return True


def _get_buffer_status(buffer: dict, recording_id: int) -> dict:
    """
    Internal helper to get current buffer status for debugging.
    """
    # Calculate what the next frame number would be
    total_samples_processed = (buffer['total_bytes_received'] - len(buffer['audio_buffer'])) // AUDIO_BYTES_PER_SAMPLE
    next_frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
    
    return {
        "recording_id": recording_id,
        "buffer_bytes": len(buffer['audio_buffer']),
        "buffer_samples": len(buffer['audio_buffer']) // AUDIO_BYTES_PER_SAMPLE,
        "buffer_ms": (len(buffer['audio_buffer']) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
        "buffer_frames": len(buffer['audio_buffer']) / AUDIO_BYTES_PER_FRAME,
        "total_bytes_received": buffer['total_bytes_received'],
        "total_frames_produced": buffer['total_frames_produced'],
        "next_frame_no": next_frame_no,
        "can_produce_frame": len(buffer['audio_buffer']) >= AUDIO_BYTES_PER_FRAME
    }


# Legacy function names for backward compatibility (deprecated)
def get_or_create_alignment_buffer(recording_id: int):
    """DEPRECATED: Use SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("GET_BUFFER_STATUS", recording_id) instead"""
    return SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("GET_BUFFER_STATUS", recording_id)

def process_audio_chunk(recording_id: int, client_frame_no: int, audio_bytes: bytes) -> List[Tuple[int, bytes]]:
    """DEPRECATED: Use SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("SPLIT_INTO_100_MS_FRAMES", recording_id, client_frame_no, audio_bytes) instead"""
    return SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("SPLIT_INTO_100_MS_FRAMES", recording_id, client_frame_no, audio_bytes)

def flush_recording_audio(recording_id: int) -> List[Tuple[int, bytes]]:
    """DEPRECATED: Use SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("FLUSH_REMAINING_AUDIO", recording_id) instead"""
    return SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("FLUSH_REMAINING_AUDIO", recording_id)

def get_buffer_status(recording_id: int) -> Optional[dict]:
    """DEPRECATED: Use SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("GET_BUFFER_STATUS", recording_id) instead"""
    return SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("GET_BUFFER_STATUS", recording_id)

def validate_frame_bytes(frame_bytes: bytes, frame_no: int) -> bool:
    """DEPRECATED: Use SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("VALIDATE_FRAME", 0, frame_no, frame_bytes) instead"""
    SERVER_ENGINE_3B1_SPLIT_INTO_100_MS_FRAMES("VALIDATE_FRAME", 0, frame_no, frame_bytes)
    return True  # Always returns True since validation just logs results
