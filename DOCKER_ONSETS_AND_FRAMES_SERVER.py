# ------------------------------------------------------------
# DOCKER_ONSETS_AND_FRAMES_SERVER.py
# A standalone Flask microservice that loads Magenta Onsets & Frames once,
# then serves /transcribe requests quickly.
#
# Run inside Docker (recommended):
#   python DOCKER_ONSETS_AND_FRAMES_SERVER.py
# Exposes: POST /transcribe  (raw WAV/FLAC/MP3/OGG bytes)
# Returns: {"notes":[{"onset_sec":..,"offset_sec":..,"pitch_midi":..,"velocity":..}, ...]}
# ------------------------------------------------------------
import os
import io
import numpy as np
import soundfile as sf
from flask import Flask, request, jsonify
from flask_cors import CORS

# Magenta / TF imports (heavy)
from magenta.models.onsets_frames_transcription import configs, infer
import tensorflow as tf

MODEL_DIR = os.getenv("OAF_MODEL_DIR", "/models/onsets_frames")
TARGET_SR = 16000

app = Flask(__name__)
CORS(app)

print("🔄 Loading Onsets & Frames model from:", MODEL_DIR, flush=True)
config = configs.CONFIG_MAP["onsets_frames"]
hparams = config.hparams
hparams.parse("batch_size=1")
checkpoint_path = tf.train.latest_checkpoint(MODEL_DIR)
if not checkpoint_path:
    raise RuntimeError(f"No checkpoint found in {MODEL_DIR}")

# Note: infer.transcribe_audio handles inference
print("✅ Model ready at checkpoint:", checkpoint_path, flush=True)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "model": "Onsets & Frames", "checkpoint": checkpoint_path})

@app.route("/transcribe", methods=["POST"])
def transcribe():
    try:
        if not request.data:
            return jsonify({"error": "no audio bytes"}), 400

        audio, sr = sf.read(io.BytesIO(request.data), always_2d=False)
        # Convert to mono
        if audio.ndim > 1:
            audio = np.mean(audio, axis=1)

        # Resample to TARGET_SR if needed
        if sr != TARGET_SR:
            # avoid importing librosa here for speed; soundfile doesn't resample
            # if you prefer librosa, you can add it and resample:
            import librosa
            audio = librosa.resample(audio, orig_sr=sr, target_sr=TARGET_SR)
            sr = TARGET_SR

        # Run inference
        # Returns list of pretty_midi.Note-like dicts:
        # [{"onset_time": s, "offset_time": s, "pitch": int, "velocity": int}, ...]
        notes_list, _ = infer.transcribe_audio(audio, hparams, checkpoint_path)

        # Normalize output to our field names
        out = []
        for n in notes_list:
            out.append({
                "onset_sec": float(n["onset_time"]),
                "offset_sec": float(n["offset_time"]),
                "pitch_midi": int(n["pitch"]),
                "velocity": int(n.get("velocity", 64)),
            })
        return jsonify({"notes": out})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Default port 8500 inside the container
    app.run(host="0.0.0.0", port=8500)
