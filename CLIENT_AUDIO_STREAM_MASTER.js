// CLIENT_AUDIO_STREAM_MASTER.js
// WebSocket audio streaming client for VIOLIN_MVP
// Protocol per frame: send TEXT meta (JSON) then BINARY (or base64-on-Android) audio bytes.
// Fields now: MESSAGE_TYPE, RECORDING_ID, FRAME_NO, FRAME_DURATION_IN_MS, BYTES_LEN.

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
// Backend helpers (generic /INSERT_TABLE/{table})
// ─────────────────────────────────────────────────────────────
function _backendBase() {
  const base = String(CLIENT_APP_VARIABLES.BACKEND_URL || '').replace(/\/+$/, '');
  return base || null;
}
function INSERT_TABLE(table, rowOrRows) {
  try {
    const base = _backendBase();
    if (!base) return;
    const url = `${base}/INSERT_TABLE/${encodeURIComponent(table)}`;
    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      keepalive: true,
      body: JSON.stringify(rowOrRows),
    }).catch(() => {});
  } catch {}
}

// Convenience specific loggers (all non-blocking)
function LOG_CLIENT_CONN_OPEN({ recordingId }) {
  INSERT_TABLE('CLIENT_DB_LOG_WEBSOCKET_CONNECTION', {
    RECORDING_ID: String(recordingId),
    DT_WEBSOCKET_OPEN_REQUEST_SENT: new Date().toISOString(),
  });
}
function LOG_CLIENT_CONN_BANNER({ recordingId, banner }) {
  INSERT_TABLE('CLIENT_DB_LOG_WEBSOCKET_CONNECTION', {
    RECORDING_ID: String(recordingId),
    DT_SERVER_CONFIRMATION_TEXT_RECEIVED: new Date().toISOString(),
    SERVER_CONFIRMATION_TEXT: String(banner ?? ''),
  });
}
function LOG_CLIENT_MESSAGE({ recordingId, type, frameNo }) {
  INSERT_TABLE('CLIENT_DB_LOG_WEBSOCKET_MESSAGE', {
    RECORDING_ID: String(recordingId),
    MESSAGE_TYPE: String(type),
    AUDIO_FRAME_NO: frameNo != null ? Number(frameNo) : null,
    DT_MESSAGE_SENT: new Date().toISOString(),
  });
}
function LOG_CLIENT_FRAME({ recordingId, frameNo, frameMs }) {
  const n = Number(frameNo);
  const startMs = (n - 1) * Number(frameMs);
  const endMs = startMs + Number(frameMs);
  INSERT_TABLE('CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME', {
    RECORDING_ID: String(recordingId),
    AUDIO_FRAME_NO: n,
    START_MS: startMs,
    END_MS: endMs,
    DT_FRAME_SENT: new Date().toISOString(),
  });
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
// Frame size (from DB via CLIENT_APP_VARIABLES)
const FRAME_MS = Number(CLIENT_APP_VARIABLES.AUDIO_STREAM_FRAME_SIZE_IN_MS) || 250;
const RESEND_BUFFER_SIZE = 128;
const SEND_SLACK_MS = 15;

let WS = null;
let STREAMING = false;

let FRAME_NO = 1;
let COUNTDOWN_REMAINING_MS = 0;
let BOUNDARY_SENT = false;

// NEW: single active recorder + non-overlapping tick scheduling
let _rec = null;                 // Audio.Recording
let _isChunking = false;         // guard against overlap
let _nextTimeout = null;         // handle for setTimeout chain

// Conductor UI countdown timer
let _countdownTimer = null;

// Banner guard (log once)
let _bannerLogged = false;

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

// JSON safe parse
function JSON_PARSE_SAFE(raw) {
  try {
    return JSON.parse(raw, (k, v) => (k === 'RECORDING_ID' ? String(v) : v));
  } catch {
    return null;
  }
}

// NEW: micro-chunk using single active recorder, race-safe with STOP
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

    const recRef = _rec;
    await new Promise((r) => setTimeout(r, ms));

    if (!STREAMING) return null;
    if (!recRef || recRef !== _rec) return null;

    try {
      await recRef.stopAndUnloadAsync();
    } catch (e) {
      WARN('stopAndUnloadAsync failed (likely due to STOP race); ignoring.', String(e));
      return null;
    }
    const uri = recRef.getURI();
    LOG('Recorded micro-chunk', { uri });

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
    MESSAGE_TYPE: 'FRAME',
    RECORDING_ID: String(recordingId),
    FRAME_NO: String(frameNo), // send as string
    FRAME_DURATION_IN_MS: frameMs,
    BYTES_LEN: bytes.byteLength,
  };

  WS_SEND_JSON(header);
  WS.send(bytes);

  // Client-side DB logs (non-blocking)
  LOG_CLIENT_MESSAGE({
    recordingId: recordingId,
    type: 'FRAME',
    frameNo: frameNo,
  });
  LOG_CLIENT_FRAME({
    recordingId: recordingId,
    frameNo: frameNo,
    frameMs: frameMs,
  });
}

// ========= resolves on onopen OR first banner message =========
async function WS_OPEN_WITH_TIMEOUT(url, timeoutMs, { subprotocols } = {}) {
  return new Promise((resolve, reject) => {
    let opened = false;
    let ws;
    let poll = null;

    const finish = (ok, val) => {
      if (opened) return;
      opened = true;
      try { clearTimeout(timer); } catch {}
      try { if (poll) clearInterval(poll); } catch {}
      try { if (ws) ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null; } catch {}
      ok ? resolve(val) : reject(val);
    };

    try {
      ws = subprotocols ? new WebSocket(url, subprotocols) : new WebSocket(url);
      ws.binaryType = 'arraybuffer';
    } catch (e) {
      return reject(e);
    }

    // Poll readyState (RN sometimes drops onopen)
    poll = setInterval(() => {
      try {
        if (ws && ws.readyState === 1) finish(true, ws);
      } catch {}
    }, 50);

    const timer = setTimeout(() => {
      try { ws.close(); } catch {}
      finish(false, new Error(`WS open timeout: ${url}`));
    }, timeoutMs);

    ws.onopen = () => {
      finish(true, ws);
    };

    ws.onmessage = (ev) => {
      const txt = typeof ev?.data === 'string' ? ev.data : '';
      // Accept either echo banner, stream banner, or any first text as proof of open.
      if (txt.startsWith('echo-server') || txt.startsWith('stream-server') || txt.length > 0) {
        finish(true, ws);
      }
    };

    ws.onerror = () => { /* let timeout/onclose decide */ };

    ws.onclose = () => {
      if (!opened) finish(false, new Error(`WS closed before open: ${url}`));
    };
  });
}

// Retry wrapper for stream open
async function WS_OPEN_WITH_RETRIES(url, timeoutMs, attempts = 2, backoffMs = 400) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      return await WS_OPEN_WITH_TIMEOUT(url, timeoutMs);
    } catch (e) {
      lastErr = e;
      if (i < attempts - 1) {
        await new Promise(r => setTimeout(r, backoffMs * (i + 1)));
      }
    }
  }
  throw lastErr;
}

// ======================================================================

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
  MARK_UI_DIRTY();

  const WS_URL = GET_WS_URL();
  const ECHO_URL = GET_WS_ECHO_URL();
  if (!WS_URL) {
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

  // Log open request (client-side)
  _bannerLogged = false;
  LOG_CLIENT_CONN_OPEN({ recordingId: RECORDING_ID });

  // 1) Echo preflight — BEST EFFORT (do not abort if it fails)
  try {
    if (ECHO_URL) {
      const echoWS = await WS_OPEN_WITH_TIMEOUT(ECHO_URL, 6000);
      LOG('Echo WS open ✓');
      try { echoWS.close(); } catch {}
    }
  } catch (e) {
    WARN('Echo WS failed (continuing to /ws/stream)', String(e));
  }

  // 2) Real streaming WS (6s, with 2 attempts)
  LOG('WS → connecting (stream)', { WS_URL, RECORDING_ID, AUDIO_STREAM_FILE_NAME });
  try {
    WS = await WS_OPEN_WITH_RETRIES(WS_URL, 6000, 2, 500);
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

      // Log the very first banner text we receive from server (once)
      if (isText && !_bannerLogged) {
        _bannerLogged = true;
        LOG_CLIENT_CONN_BANNER({
          recordingId: RECORDING_ID,
          banner: String(payload),
        });
      }
    } catch {}
    try {
      const raw = typeof evt.data === 'string'
        ? evt.data
        : new TextDecoder().decode(evt.data);
      const msg = JSON_PARSE_SAFE(raw) || {};
      if (msg.MESSAGE_TYPE === 'ACK') {
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
  WS_SEND_JSON({ MESSAGE_TYPE: 'START', RECORDING_ID, AUDIO_STREAM_FILE_NAME });
  LOG_CLIENT_MESSAGE({
    recordingId: RECORDING_ID,
    type: 'START',
    frameNo: null,
  });

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
        return;
      }

      const audioBytes = await READ_FILE_AS_UINT8(uri);

      // Discard every chunk until countdown completes exactly on a frame boundary.
      if (!BOUNDARY_SENT) {
        if (COUNTDOWN_REMAINING_MS > 0) {
          COUNTDOWN_REMAINING_MS -= FRAME_MS;
          LOG('Countdown chunk discarded', { COUNTDOWN_REMAINING_MS });
          if (COUNTDOWN_REMAINING_MS <= 0) {
            BOUNDARY_SENT = true;
            FRAME_NO = 1;
            LOG('Countdown finished; next chunk will be frame #1');
          }
          try { await FileSystem.deleteAsync(uri, { idempotent: true }); } catch {}
          return;
        } else {
          BOUNDARY_SENT = true;
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
      WS_SEND_JSON({ MESSAGE_TYPE: 'STOP', RECORDING_ID });
      LOG_CLIENT_MESSAGE({
        recordingId: RECORDING_ID,
        type: 'STOP',
        frameNo: null,
      });
    }
  } catch {}

  try { WS && WS.close(); } catch {}
  WS = null;

  RESEND_BUFFER.clear();
  LOG('Streaming stopped');
  try { MIRROR_FLUSH_NOW(); } catch {}
  MARK_UI_DIRTY();
}
