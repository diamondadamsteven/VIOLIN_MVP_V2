// CLIENT_AUDIO_STREAM_MASTER.js
// WebSocket audio streaming client for VIOLIN_MVP
// Protocol per frame: send TEXT meta (JSON) then BINARY audio bytes.
// Fields now: RECORDING_ID, FRAME_NO, FRAME_DURATION_IN_MS, BYTES_LEN.

import { Audio } from 'expo-av';
import * as FileSystem from 'expo-file-system';
import { DeviceEventEmitter } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

// ─────────────────────────────────────────────────────────────
// Phone → Backend console mirror
// ─────────────────────────────────────────────────────────────
const MIRROR_ENABLED = true;
const MIRROR_BATCH_MAX = 20;
const MIRROR_FLUSH_MS = 500;

let _mirrorQueue = [];
let _mirrorTimer = null;

function MIRROR_FLUSH_NOW() {
  try {
    if (!MIRROR_ENABLED || _mirrorQueue.length === 0) return;
    const base = String(CLIENT_APP_VARIABLES.BACKEND_URL || '').replace(/\/+$/, '');
    if (!base) return;
    const url = `${base}/CLIENT_LOG`;
    const batch = _mirrorQueue.splice(0, MIRROR_BATCH_MAX);
    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      keepalive: true,
      body: JSON.stringify({ entries: batch }),
    }).catch(() => {});
  } catch {}
}

function PHONELOG(level, tag, msg, extra) {
  try {
    const entry = {
      t: new Date().toISOString(),
      level,
      tag,
      msg: String(msg ?? ''),
      extra: extra ?? null,
    };
    _mirrorQueue.push(entry);
    if (_mirrorQueue.length >= MIRROR_BATCH_MAX) MIRROR_FLUSH_NOW();
    if (!_mirrorTimer) {
      _mirrorTimer = setInterval(() => {
        if (_mirrorQueue.length === 0) return;
        MIRROR_FLUSH_NOW();
      }, MIRROR_FLUSH_MS);
    }
  } catch {}
}

function LOG(msg, obj) {
  const prefix = 'CLIENT_AUDIO_STREAM_MASTER';
  if (obj !== undefined) {
    console.log(`${prefix} - ${msg}`, obj);
    PHONELOG('INFO', prefix, msg, obj);
  } else {
    console.log(`${prefix} - ${msg}`);
    PHONELOG('INFO', prefix, msg, null);
  }
}
function WARN(msg, obj) {
  const prefix = 'CLIENT_AUDIO_STREAM_MASTER';
  if (obj !== undefined) {
    console.warn(`${prefix} - ${msg}`, obj);
    PHONELOG('WARN', prefix, msg, obj);
  } else {
    console.warn(`${prefix} - ${msg}`);
    PHONELOG('WARN', prefix, msg, null);
  }
}
function ERR(msg, obj) {
  const prefix = 'CLIENT_AUDIO_STREAM_MASTER';
  if (obj !== undefined) {
    console.error(`${prefix} - ${msg}`, obj);
    PHONELOG('ERROR', prefix, msg, obj);
  } else {
    console.error(`${prefix} - ${msg}`);
    PHONELOG('ERROR', prefix, msg, null);
  }
}

// Let UI know to re-render when we flip flags
function MARK_UI_DIRTY() {
  try { DeviceEventEmitter.emit('EVT_UI_DIRTY'); } catch {}
}

// ─────────────────────────────────────────────────────────────
// URL helpers
// ─────────────────────────────────────────────────────────────
function _baseHost() {
  const base = String(CLIENT_APP_VARIABLES.BACKEND_URL || '').replace(/\/+$/, '');
  if (!base) return null;
  const u = new URL(base);
  return { proto: u.protocol, host: u.hostname };
}
function GET_WS_URL() {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.GET_WS_URL');
  try {
    const h = _baseHost();
    if (!h) { WARN('BACKEND_URL not set'); return null; }
    const wsProto = h.proto === 'https:' ? 'wss:' : 'ws:';
    const url = `${wsProto}//${h.host}:7070/ws/stream`;
    LOG('WS URL', { url });
    return url;
  } catch (e) {
    WARN('Invalid BACKEND_URL', { BACKEND_URL: CLIENT_APP_VARIABLES.BACKEND_URL, error: String(e) });
    return null;
  }
}
function GET_WS_ECHO_URL() {
  try {
    const h = _baseHost();
    if (!h) return null;
    const wsProto = h.proto === 'https:' ? 'wss:' : 'ws:';
    const url = `${wsProto}//${h.host}:7070/ws/echo`;
    LOG('WS ECHO URL', { url });
    return url;
  } catch {
    return null;
  }
}
function GET_HEALTH_URL() {
  try {
    const h = _baseHost();
    if (!h) return null;
    const httpProto = h.proto === 'https:' ? 'https:' : 'http:';
    return `${httpProto}//${h.host}:7070/health`;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────────────────────
// Frame size is now set dynamically (from DB via CLIENT_APP_VARIABLES)
const FRAME_MS = Number(CLIENT_APP_VARIABLES.AUDIO_STREAM_FRAME_SIZE_IN_MS) || 250;
const RESEND_BUFFER_SIZE = 128;
const SEND_SLACK_MS = 15;

let WS = null;
let STREAMING = false;

let FRAME_NO = 1;                 // frames start at 1; no 0/negatives sent
let COUNTDOWN_REMAINING_MS = 0;
let BOUNDARY_SENT = false;

// NEW: single active recorder + non-overlapping tick scheduling
let _rec = null;                 // Audio.Recording
let _isChunking = false;         // guard against overlap
let _nextTimeout = null;         // handle for setTimeout chain

// ── Conductor UI countdown timer (non-blocking)
let _countdownTimer = null;

// Resend buffer
const RESEND_BUFFER = new Map();
function RESEND_BUFFER_PUT(frameNo, entry) {
  RESEND_BUFFER.set(Number(frameNo), entry);
  if (RESEND_BUFFER.size > RESEND_BUFFER_SIZE) {
    const oldest = RESEND_BUFFER.keys().next().value;
    RESEND_BUFFER.delete(oldest);
  }
}
function RESEND_BUFFER_GET(frameNo) {
  return RESEND_BUFFER.get(Number(frameNo));
}

// Base64 → bytes
function BASE64_TO_BYTES(b64) {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.BASE64_TO_BYTES');
  const lookup = new Uint8Array(256);
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  for (let i = 0; i < alphabet.length; i++) lookup[alphabet.charCodeAt(i)] = i;

  let bufferLength = Math.floor(b64.length * 0.75);
  if (b64.endsWith('==')) bufferLength -= 2;
  else if (b64.endsWith('=')) bufferLength -= 1;

  const bytes = new Uint8Array(bufferLength);
  let p = 0;

  for (let i = 0; i < b64.length; i += 4) {
    const enc1 = lookup[b64.charCodeAt(i)];
    const enc2 = lookup[b64.charCodeAt(i + 1)];
    const enc3 = lookup[b64.charCodeAt(i + 2)];
    const enc4 = lookup[b64.charCodeAt(i + 3)];

    const chunk = (enc1 << 18) | (enc2 << 12) | ((enc3 & 63) << 6) | (enc4 & 63);
    bytes[p++] = (chunk >> 16) & 255;
    if (b64[i + 2] !== '=') bytes[p++] = (chunk >> 8) & 255;
    if (b64[i + 3] !== '=') bytes[p++] = chunk & 255;
  }
  return bytes;
}

// JSON parse that preserves large IDs as strings for logging clarity
function JSON_PARSE_SAFE(raw) {
  try {
    return JSON.parse(raw, (k, v) => (k === 'RECORDING_ID' ? String(v) : v));
  } catch {
    return null;
  }
}

// NEW: micro-chunk using the single active recorder, race-safe with STOP
async function RECORD_MICRO_CHUNK(ms) {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.RECORD_MICRO_CHUNK');
  if (_isChunking) {
    LOG('RECORD_MICRO_CHUNK skipped (busy)');
    return null;
  }
  _isChunking = true;
  try {
    if (!_rec) {
      _rec = new Audio.Recording();
      await _rec.prepareToRecordAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
      await _rec.startAsync();
    }

    // Capture the recorder reference used for this slice
    const recRef = _rec;

    // Wait for the slice to elapse
    await new Promise((r) => setTimeout(r, ms));

    // If stop was pressed during the wait, or the recorder changed, bail
    if (!STREAMING) return null;
    if (!recRef || recRef !== _rec) return null;

    // Stop current slice and get URI
    try {
      await recRef.stopAndUnloadAsync();
    } catch (e) {
      // If it was already stopped elsewhere, just bail
      WARN('stopAndUnloadAsync failed (likely due to STOP race); ignoring.', String(e));
      return null;
    }
    const uri = recRef.getURI();
    LOG('Recorded micro-chunk', { uri });

    // Immediately start the next slice if we're still streaming and the active recorder is still this one
    if (STREAMING && recRef === _rec) {
      _rec = new Audio.Recording();
      await _rec.prepareToRecordAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
      await _rec.startAsync();
    }

    return uri;
  } catch (e) {
    ERR('RECORD_MICRO_CHUNK error', String(e));
    return null;
  } finally {
    _isChunking = false;
  }
}

async function READ_FILE_AS_UINT8(uri) {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.READ_FILE_AS_UINT8');
  const base64 = await FileSystem.readAsStringAsync(uri, { encoding: FileSystem.EncodingType.Base64 });
  const bytes = BASE64_TO_BYTES(base64);
  return bytes;
}

function WS_SEND_JSON(obj) {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.WS_SEND_JSON');
  if (WS && WS.readyState === 1) {
    WS.send(JSON.stringify(obj));
  }
}

async function SEND_FRAME_PAIR({ recordingId, frameNo, frameMs, bytes }) {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.SEND_FRAME_PAIR');
  if (!WS || WS.readyState !== 1) return;

  const header = {
    type: 'FRAME',
    RECORDING_ID: String(recordingId),
    FRAME_NO: String(frameNo), // send as string
    FRAME_DURATION_IN_MS: frameMs,
    BYTES_LEN: bytes.byteLength,
  };

  WS_SEND_JSON(header);
  WS.send(bytes);
}

async function WS_OPEN_WITH_TIMEOUT(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    let opened = false;
    let ws;
    try {
      ws = new WebSocket(url);
      ws.binaryType = 'arraybuffer';
    } catch (e) {
      return reject(e);
    }

    const timer = setTimeout(() => {
      if (!opened) {
        try { ws.close(); } catch {}
        reject(new Error(`WS open timeout: ${url}`));
      }
    }, timeoutMs);

    ws.onopen = () => {
      opened = true;
      clearTimeout(timer);
      resolve(ws);
    };
    ws.onerror = (evt) => {
      ERR('WS error during open', { url, evt });
    };
    ws.onclose = (evt) => {
      if (!opened) ERR('WS closed before open', { url, code: evt.code, reason: evt.reason });
    };
  });
}

// Conductor countdown — non-blocking & visible even while frames stream
function START_CONDUCTOR_COUNTDOWN(beats, bpm) {
  try { if (_countdownTimer) clearInterval(_countdownTimer); } catch {}
  const perBeat = 60000 / Math.max(1, Number(bpm) || 60);
  let i = Math.max(0, Number(beats) || 0);
  if (i === 0) return;
  // Initial tick immediately
  DeviceEventEmitter.emit('EVT_CONDUCTOR_UPDATED', {
    CONDUCTOR_MESSAGE_TEXT: `Start in ${i}…`,
    CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL: 'NEUTRAL',
    CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS: Math.min(1000, perBeat),
  });
  _countdownTimer = setInterval(() => {
    i -= 1;
    if (i > 0) {
      DeviceEventEmitter.emit('EVT_CONDUCTOR_UPDATED', {
        CONDUCTOR_MESSAGE_TEXT: `Start in ${i}…`,
        CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL: 'NEUTRAL',
        CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS: Math.min(1000, perBeat),
      });
    } else {
      clearInterval(_countdownTimer);
      _countdownTimer = null;
      DeviceEventEmitter.emit('EVT_CONDUCTOR_UPDATED', {
        CONDUCTOR_MESSAGE_TEXT: 'Recording...',
        CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL: 'NEUTRAL',
        CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS: 2000,
      });
    }
  }, perBeat);
}

export async function START_STREAMING_WS({ countdownBeats = 0, bpm = 60 }) {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.START_STREAMING_WS', { FRAME_MS });
  if (STREAMING) return;
  STREAMING = true;
  MARK_UI_DIRTY(); // force buttons to update immediately (Stop only)

  const WS_URL = GET_WS_URL();
  const ECHO_URL = GET_WS_ECHO_URL();
  if (!WS_URL || !ECHO_URL) {
    STREAMING = false;
    MARK_UI_DIRTY();
    return;
  }

  // Probe /health first
  try {
    const health = GET_HEALTH_URL();
    if (health) {
      const r = await fetch(health, { method: 'GET' });
      const text = await r.text();
      LOG('Listener /health', { status: r.status, text });
    }
  } catch (e) {
    WARN('Listener /health probe failed (still trying WS)', { error: String(e) });
  }

  // Mic perms + audio mode
  try {
    const perm = await Audio.requestPermissionsAsync();
    LOG('Mic permission result', perm);
    await Audio.setAudioModeAsync({
      allowsRecordingIOS: true,
      playsInSilentModeIOS: true,
    });
  } catch (e) {
    ERR('Audio permission/mode error', String(e));
    STREAMING = false;
    MARK_UI_DIRTY();
    return;
  }

  const RECORDING_ID = String(CLIENT_APP_VARIABLES.RECORDING_ID || '');
  if (!RECORDING_ID) {
    WARN('No RECORDING_ID set in CLIENT_APP_VARIABLES.');
    STREAMING = false;
    MARK_UI_DIRTY();
    return;
  }
  const AUDIO_STREAM_FILE_NAME = String(CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME || '');
  LOG('Preflight echo connect');

  // 1) Echo preflight (4s)
  let echoWS = null;
  try {
    echoWS = await WS_OPEN_WITH_TIMEOUT(GET_WS_ECHO_URL(), 4000);
    LOG('Echo WS open ✓');
  } catch (e) {
    ERR('Echo WS failed to open', String(e));
    STREAMING = false;
    MARK_UI_DIRTY();
    return;
  }
  try { echoWS.close(); } catch {}
  echoWS = null;

  // 2) Real streaming WS (6s)
  LOG('WS → connecting (stream)', { WS_URL, RECORDING_ID, AUDIO_STREAM_FILE_NAME });
  try {
    WS = await WS_OPEN_WITH_TIMEOUT(WS_URL, 6000);
  } catch (e) {
    ERR('Stream WS failed to open', String(e));
    STREAMING = false;
    MARK_UI_DIRTY();
    return;
  }

  WS.onmessage = (evt) => {
    try {
      const isText = typeof evt.data === 'string';
      const payload = isText ? evt.data : '<binary>';
      LOG('WS onmessage', { preview: String(payload).slice(0, 200) });
    } catch {}
    try {
      const raw = typeof evt.data === 'string'
        ? evt.data
        : new TextDecoder().decode(evt.data);
      const msg = JSON_PARSE_SAFE(raw) || {};
      if (msg.type === 'ACK') {
        const missing = Array.isArray(msg.MISSING_FRAMES) ? msg.MISSING_FRAMES : [];
        if (missing.length) LOG('Resend requested', { missing });
        for (const m of missing) {
          const entry = RESEND_BUFFER_GET(m);
          if (entry) {
            SEND_FRAME_PAIR({
              recordingId: RECORDING_ID,
              frameNo: m,
              frameMs: entry.header.FRAME_DURATION_IN_MS,
              bytes: entry.bytes,
            });
          }
        }
      } else {
        LOG('WS message (parsed)', msg);
      }
    } catch {}
  };
  WS.onclose = (evt) => {
    WARN('WS close', { code: evt.code, reason: evt.reason });
  };
  WS.onerror = (evt) => {
    ERR('WS error (after open)', { evt });
  };

  // Send START
  WS_SEND_JSON({ type: 'START', RECORDING_ID, AUDIO_STREAM_FILE_NAME });

  const MS_PER_BEAT = 60000 / Math.max(1, bpm);
  COUNTDOWN_REMAINING_MS = Math.max(0, Math.round(countdownBeats * MS_PER_BEAT));
  FRAME_NO = 1;
  BOUNDARY_SENT = COUNTDOWN_REMAINING_MS === 0;

  // Kick the visible conductor countdown (non-blocking)
  START_CONDUCTOR_COUNTDOWN(countdownBeats, bpm);

  // Prime the first recording slice so the first tick can stop/unload it
  _rec = new Audio.Recording();
  await _rec.prepareToRecordAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
  await _rec.startAsync();

  // Single-flight arming helper (ensure only one next tick)
  const ARM_NEXT = (delayMs) => {
    if (_nextTimeout !== null) return; // already armed
    _nextTimeout = setTimeout(async () => {
      _nextTimeout = null;
      await TICK();
    }, delayMs);
  };

  // Non-overlapping tick loop
  const TICK = async () => {
    if (!STREAMING || !WS || WS.readyState !== 1) return;

    try {
      const uri = await RECORD_MICRO_CHUNK(FRAME_MS);
      if (!uri) {
        // Busy or race/stop; finally will ARM_NEXT() once.
        return;
      }

      const audioBytes = await READ_FILE_AS_UINT8(uri);

      // Discard every chunk until countdown completes exactly on a frame boundary.
      if (!BOUNDARY_SENT) {
        if (COUNTDOWN_REMAINING_MS > 0) {
          COUNTDOWN_REMAINING_MS -= FRAME_MS;
          LOG('Countdown chunk discarded', { COUNTDOWN_REMAINING_MS });
          if (COUNTDOWN_REMAINING_MS <= 0) {
            // Countdown finished at this frame boundary; next chunk becomes frame #1
            BOUNDARY_SENT = true;
            FRAME_NO = 1;
            LOG('Countdown finished; next chunk will be frame #1');
          }
          try { await FileSystem.deleteAsync(uri, { idempotent: true }); } catch {}
          return; // finally will ARM_NEXT()
        } else {
          BOUNDARY_SENT = true; // (in case countdownBeats was 0)
        }
      }

      // Send a regular frame
      const frameNoToSend = FRAME_NO;
      FRAME_NO += 1;

      RESEND_BUFFER_PUT(frameNoToSend, {
        header: { FRAME_DURATION_IN_MS: FRAME_MS },
        bytes: audioBytes,
      });

      await SEND_FRAME_PAIR({
        recordingId: RECORDING_ID,
        frameNo: frameNoToSend,
        frameMs: FRAME_MS,
        bytes: audioBytes,
      });

      try { await FileSystem.deleteAsync(uri, { idempotent: true }); } catch {}
    } catch (e) {
      ERR('Streaming loop error', String(e));
    } finally {
      if (STREAMING) ARM_NEXT(FRAME_MS + SEND_SLACK_MS);
    }
  };

  // Kick off loop
  ARM_NEXT(FRAME_MS + SEND_SLACK_MS);
}

export async function STOP_STREAMING_WS() {
  LOG('Start function CLIENT_AUDIO_STREAM_MASTER.STOP_STREAMING_WS');
  if (!STREAMING) return;
  STREAMING = false;

  if (_nextTimeout) { clearTimeout(_nextTimeout); _nextTimeout = null; }
  if (_countdownTimer) { try { clearInterval(_countdownTimer); } catch {} _countdownTimer = null; }

  // Race-safe: detach current recorder, then stop it
  let recRef = _rec;
  _rec = null;
  _isChunking = false;
  try {
    if (recRef) {
      try { await recRef.stopAndUnloadAsync(); } catch {}
    }
  } catch {}

  try {
    if (WS && WS.readyState === 1) {
      const RECORDING_ID = String(CLIENT_APP_VARIABLES.RECORDING_ID || '');
      WS_SEND_JSON({ type: 'STOP', RECORDING_ID });
    }
  } catch {}

  try { WS && WS.close(); } catch {}
  WS = null;

  RESEND_BUFFER.clear();
  LOG('Streaming stopped');
  try { MIRROR_FLUSH_NOW(); } catch {}
  MARK_UI_DIRTY();
}
