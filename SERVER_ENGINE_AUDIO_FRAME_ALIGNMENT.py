# SERVER_ENGINE_AUDIO_FRAME_ALIGNMENT.py
"""
Audio Frame Alignment System

This module handles the alignment of variable-length audio chunks from the client
into exact 100ms frames for processing. It accumulates audio data and produces
properly sized frames that can be processed by the analysis pipeline.
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


def get_or_create_alignment_buffer(recording_id: int):
    """Get or create an audio frame alignment buffer for a recording."""
    if recording_id not in AUDIO_FRAME_ALIGNMENT_BUFFERS:
        AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id] = AudioFrameAlignmentBuffer(recording_id)
        CONSOLE_LOG(PREFIX, "created_buffer", {
            "rid": recording_id,
            "target_frame_ms": AUDIO_FRAME_MS,
            "target_sample_rate": AUDIO_SAMPLE_RATE,
            "target_bytes_per_frame": AUDIO_BYTES_PER_FRAME
        })
    
    return AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id]


def process_audio_chunk(recording_id: int, client_frame_no: int, audio_bytes: bytes) -> List[Tuple[int, bytes]]:
    """
    Process an audio chunk from the client and return any complete frames that can be produced.
    
    Args:
        recording_id: The recording ID
        client_frame_no: The frame number from the client (for logging)
        audio_bytes: The raw audio bytes from the client
        
    Returns:
        List of (frame_no, frame_bytes) tuples for complete frames
    """
    buffer = get_or_create_alignment_buffer(recording_id)
    
    # Log the incoming chunk
    CONSOLE_LOG(PREFIX, "received_chunk", {
        "rid": recording_id,
        "client_frame": client_frame_no,
        "chunk_bytes": len(audio_bytes),
        "chunk_samples": len(audio_bytes) // AUDIO_BYTES_PER_SAMPLE,
        "chunk_ms": (len(audio_bytes) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
        "chunk_bytes_even": len(audio_bytes) % 2 == 0
    })
    
    # Add the chunk to the buffer and get any complete frames
    complete_frames = buffer.add_audio_chunk(audio_bytes, client_frame_no)
    
    # Log buffer status after processing
    buffer_status = buffer.get_buffer_status()
    CONSOLE_LOG(PREFIX, "buffer_status", buffer_status)
    
    # Log any complete frames produced
    if complete_frames:
        CONSOLE_LOG(PREFIX, "produced_frames", {
            "rid": recording_id,
            "frames_produced": len(complete_frames),
            "frame_numbers": [f[0] for f in complete_frames],
            "total_bytes_in_frames": sum(len(f[1]) for f in complete_frames)
        })
    
    return complete_frames


def flush_recording_audio(recording_id: int) -> List[Tuple[int, bytes]]:
    """
    Flush any remaining audio data for a recording when it's stopped.
    This produces the final frame(s) which may be shorter than 100ms.
    
    Args:
        recording_id: The recording ID to flush
        
    Returns:
        List of (frame_no, frame_bytes) tuples for remaining frames
    """
    if recording_id not in AUDIO_FRAME_ALIGNMENT_BUFFERS:
        return []
    
    buffer = AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id]
    remaining_frames = buffer.flush_remaining_audio()
    
    if remaining_frames:
        CONSOLE_LOG(PREFIX, "flushed_remaining", {
            "rid": recording_id,
            "frames_flushed": len(remaining_frames),
            "frame_numbers": [f[0] for f in remaining_frames],
            "total_bytes_flushed": sum(len(f[1]) for f in remaining_frames)
        })
    
    # Clean up the buffer
    del AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id]
    CONSOLE_LOG(PREFIX, "cleaned_up_buffer", {"rid": recording_id})
    
    return remaining_frames


def get_buffer_status(recording_id: int) -> Optional[dict]:
    """Get the current status of an audio frame alignment buffer."""
    if recording_id not in AUDIO_FRAME_ALIGNMENT_BUFFERS:
        return None
    
    return AUDIO_FRAME_ALIGNMENT_BUFFERS[recording_id].get_buffer_status()


def validate_frame_bytes(frame_bytes: bytes, frame_no: int) -> bool:
    """
    Validate that a frame has the correct properties.
    
    Args:
        frame_bytes: The frame bytes to validate
        frame_no: The frame number for logging
        
    Returns:
        True if valid, False otherwise
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


class AudioFrameAlignmentBuffer:
    """
    Manages audio frame alignment for a single recording.
    Accumulates variable-length audio chunks and produces exact 100ms frames.
    """
    
    def __init__(self, recording_id: int):
        self.recording_id = recording_id
        self.audio_buffer = bytearray()  # Accumulated audio bytes
        self.total_bytes_received = 0
        self.total_frames_produced = 0
        self.last_frame_time = None
        
    def add_audio_chunk(self, audio_bytes: bytes, client_frame_no: int) -> List[Tuple[int, bytes]]:
        """
        Add a new audio chunk and return any complete frames that can be produced.
        
        Args:
            audio_bytes: The audio bytes to add
            client_frame_no: The client frame number (for logging)
            
        Returns:
            List of (frame_no, frame_bytes) tuples for complete frames
        """
        self.audio_buffer.extend(audio_bytes)
        self.total_bytes_received += len(audio_bytes)
        
        complete_frames = []
        
        # Keep producing frames while we have enough data
        while len(self.audio_buffer) >= AUDIO_BYTES_PER_FRAME:
            # Extract exactly one frame
            frame_bytes = bytes(self.audio_buffer[:AUDIO_BYTES_PER_FRAME])
            self.audio_buffer = self.audio_buffer[AUDIO_BYTES_PER_FRAME:]
            
            # Calculate time-based frame number
            # Each frame represents 100ms, so frame number = total samples processed / samples per frame
            total_samples_processed = (self.total_bytes_received - len(self.audio_buffer)) // AUDIO_BYTES_PER_SAMPLE
            frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
            
            self.total_frames_produced += 1
            
            # Validate the frame before returning it
            if validate_frame_bytes(frame_bytes, frame_no):
                complete_frames.append((frame_no, frame_bytes))
            else:
                CONSOLE_LOG(PREFIX, "frame_validation_failed_skipping", {
                    "rid": self.recording_id,
                    "frame": frame_no,
                    "client_frame": client_frame_no
                })
        
        return complete_frames
    
    def get_buffer_status(self) -> dict:
        """Get current buffer status for debugging."""
        # Calculate what the next frame number would be
        total_samples_processed = (self.total_bytes_received - len(self.audio_buffer)) // AUDIO_BYTES_PER_SAMPLE
        next_frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
        
        return {
            "recording_id": self.recording_id,
            "buffer_bytes": len(self.audio_buffer),
            "buffer_samples": len(self.audio_buffer) // AUDIO_BYTES_PER_SAMPLE,
            "buffer_ms": (len(self.audio_buffer) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
            "buffer_frames": len(self.audio_buffer) / AUDIO_BYTES_PER_FRAME,
            "total_bytes_received": self.total_bytes_received,
            "total_frames_produced": self.total_frames_produced,
            "next_frame_no": next_frame_no,
            "can_produce_frame": len(self.audio_buffer) >= AUDIO_BYTES_PER_FRAME
        }
    
    def flush_remaining_audio(self) -> List[Tuple[int, bytes]]:
        """
        Flush any remaining audio data as the final frame(s).
        This may produce a frame shorter than 100ms.
        
        Returns:
            List of (frame_no, frame_bytes) tuples for remaining frames
        """
        if not self.audio_buffer:
            return []
        
        # Pad to even byte length if needed
        if len(self.audio_buffer) % 2 != 0:
            self.audio_buffer.append(0)  # Add padding byte
            CONSOLE_LOG(PREFIX, "flushed_remaining_frame", {
                "rid": self.recording_id,
                "original_bytes": len(self.audio_buffer) - 1,
                "padded_bytes": len(self.audio_buffer)
            })
        
        frame_bytes = bytes(self.audio_buffer)
        
        # Calculate time-based frame number for the remaining audio
        total_samples_processed = self.total_bytes_received // AUDIO_BYTES_PER_SAMPLE
        frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
        
        self.total_frames_produced += 1
        
        CONSOLE_LOG(PREFIX, "flushed_remaining_frame", {
            "rid": self.recording_id,
            "frame": frame_no,
            "bytes": len(frame_bytes),
            "samples": len(frame_bytes) // AUDIO_BYTES_PER_SAMPLE,
            "duration_ms": (len(frame_bytes) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
            "note": "final_frame_may_be_short"
        })
        
        self.audio_buffer.clear()
        return [(frame_no, frame_bytes)]
