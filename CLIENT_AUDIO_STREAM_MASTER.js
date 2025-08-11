// CLIENT_AUDIO_STREAM_MASTER.js
// WebSocket audio streaming client for VIOLIN_MVP
// Protocol per frame: send TEXT meta (JSON) then BINARY audio bytes.
// Fields: RECORDING_ID, FRAME_NO, FRAME_DURATION_IN_MS, COUNTDOWN_OVERLAP_MS, BYTES_LEN.

import { Audio } from 'expo-av';
import * as FileSystem from 'expo-file-system';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

function LOG(msg, obj) {
  const prefix = 'CLIENT_AUDIO_STREAM_MASTER';
  if (obj !== undefined) console.log(`${prefix} - ${msg}`, obj);
  else console.log(`${prefix} - ${msg}`);
}

// ─────────────────────────────────────────────────────────────
// WS URL derived from BACKEND_URL (same host, port 7070, /ws/stream)
function GET_WS_URL() {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.GET_WS_URL`);
  try {
    const base = String(CLIENT_APP_VARIABLES.BACKEND_URL || '').replace(/\/+$/, '');
    if (!base) {
      console.warn('BACKEND_URL is not set in CLIENT_APP_VARIABLES.');
      return null;
    }
    const u = new URL(base); // e.g., http://192.168.1.50:8000
    const wsProto = u.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = u.hostname;
    const port = 7070; // engine listener port
    return `${wsProto}//${host}:${port}/ws/stream`;
  } catch (e) {
    console.warn('Invalid BACKEND_URL:', CLIENT_APP_VARIABLES.BACKEND_URL);
    return null;
  }
}
// ─────────────────────────────────────────────────────────────

const FRAME_MS = 250;                 // v1 target
const RESEND_BUFFER_SIZE = 128;
const SEND_SLACK_MS = 15;

let WS = null;
let LOOP_TIMER = null;
let STREAMING = false;

let FRAME_NO = 0;
let COUNTDOWN_REMAINING_MS = 0;
let BOUNDARY_SENT = false;

// Resend buffer
const RESEND_BUFFER = new Map();
function RESEND_BUFFER_PUT(frameNo, entry) {
  RESEND_BUFFER.set(frameNo, entry);
  if (RESEND_BUFFER.size > RESEND_BUFFER_SIZE) {
    const oldest = RESEND_BUFFER.keys().next().value;
    RESEND_BUFFER.delete(oldest);
  }
}
function RESEND_BUFFER_GET(frameNo) {
  return RESEND_BUFFER.get(frameNo);
}

// Base64 → bytes
function BASE64_TO_BYTES(b64) {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.BASE64_TO_BYTES`);
  const lookup = new Uint8Array(256);
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  for (let i = 0; i < alphabet.length; i++) lookup[alphabet.charCodeAt(i)] = i;

  let bufferLength = Math.floor(b64.length * 0.75);
  if (b64[b64.length - 1] === '=') bufferLength--;
  if (b64[b64.length - 2] === '=') bufferLength--;

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

// Record micro-chunk
async function RECORD_MICRO_CHUNK(ms) {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.RECORD_MICRO_CHUNK`);
  const rec = new Audio.Recording();
  await rec.prepareToRecordAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
  await rec.startAsync();
  await new Promise((r) => setTimeout(r, ms));
  await rec.stopAndUnloadAsync();
  const uri = rec.getURI();
  LOG('Recorded micro-chunk', { uri });
  return uri;
}

async function READ_FILE_AS_UINT8(uri) {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.READ_FILE_AS_UINT8`);
  const base64 = await FileSystem.readAsStringAsync(uri, { encoding: FileSystem.EncodingType.Base64 });
  const bytes = BASE64_TO_BYTES(base64);
  return bytes;
}

function WS_SEND_JSON(obj) {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.WS_SEND_JSON`);
  if (WS && WS.readyState === 1) {
    WS.send(JSON.stringify(obj));
  }
}

async function SEND_FRAME_PAIR({ recordingId, frameNo, frameMs, overlapMs, bytes }) {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.SEND_FRAME_PAIR`);
  if (!WS || WS.readyState !== 1) return;

  WS_SEND_JSON({
    type: 'FRAME',
    RECORDING_ID: String(recordingId),
    FRAME_NO: frameNo,
    FRAME_DURATION_IN_MS: frameMs,
    COUNTDOWN_OVERLAP_MS: overlapMs || 0,
    BYTES_LEN: bytes.byteLength,
  });

  WS.send(bytes);
}

export async function START_STREAMING_WS({ countdownBeats = 0, bpm = 60 }) {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.START_STREAMING_WS`);
  if (STREAMING) return;
  STREAMING = true;

  const WS_URL = GET_WS_URL();
  if (!WS_URL) {
    STREAMING = false;
    return;
  }

  await Audio.requestPermissionsAsync();
  await Audio.setAudioModeAsync({
    allowsRecordingIOS: true,
    playsInSilentModeIOS: true,
    interruptionModeIOS: Audio.INTERRUPTION_MODE_IOS_DO_NOT_MIX,
    shouldDuckAndroid: true,
    interruptionModeAndroid: Audio.INTERRUPTION_MODE_ANDROID_DO_NOT_MIX,
  });

  const RECORDING_ID = String(CLIENT_APP_VARIABLES.RECORDING_ID || '');
  if (!RECORDING_ID) {
    console.warn('No RECORDING_ID set in CLIENT_APP_VARIABLES.');
    STREAMING = false;
    return;
  }

  const AUDIO_STREAM_FILE_NAME = String(CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME || '');
  LOG('Opening WS', { WS_URL, RECORDING_ID, AUDIO_STREAM_FILE_NAME });

  WS = new WebSocket(WS_URL);
  WS.binaryType = 'arraybuffer';

  WS.onmessage = (evt) => {
    console.log(`CLIENT_AUDIO_STREAM_MASTER - WS onmessage`);
    try {
      const msg = typeof evt.data === 'string'
        ? JSON.parse(evt.data)
        : JSON.parse(new TextDecoder().decode(evt.data));
      if (msg.type === 'ACK') {
        const missing = msg.MISSING_FRAMES || [];
        if (missing.length) LOG('Resend requested', { missing });
        for (const m of missing) {
          const entry = RESEND_BUFFER_GET(m);
          if (entry) {
            SEND_FRAME_PAIR({
              recordingId: RECORDING_ID,
              frameNo: m,
              frameMs: entry.header.FRAME_DURATION_IN_MS,
              overlapMs: entry.header.COUNTDOWN_OVERLAP_MS,
              bytes: entry.bytes,
            });
          }
        }
      } else {
        LOG('WS message', msg);
      }
    } catch {
      // ignore
    }
  };

  await new Promise((res) => {
    if (WS.readyState === 1) return res();
    const onOpen = () => { WS.removeEventListener('open', onOpen); res(); };
    WS.addEventListener('open', onOpen);
  });

  WS_SEND_JSON({ type: 'START', RECORDING_ID, AUDIO_STREAM_FILE_NAME });

  const MS_PER_BEAT = 60000 / Math.max(1, bpm);
  COUNTDOWN_REMAINING_MS = Math.max(0, Math.round(countdownBeats * MS_PER_BEAT));
  FRAME_NO = -Math.ceil(COUNTDOWN_REMAINING_MS / FRAME_MS);
  BOUNDARY_SENT = COUNTDOWN_REMAINING_MS === 0;

  // Renamed: REFRESH_LOOP_WHILE_RECORDING (the streaming loop here is the sender loop)
  async function REFRESH_LOOP_WHILE_RECORDING() {
    console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.REFRESH_LOOP_WHILE_RECORDING`);
    if (!STREAMING || !WS || WS.readyState !== 1) return;

    try {
      const uri = await RECORD_MICRO_CHUNK(FRAME_MS);
      const audioBytes = await READ_FILE_AS_UINT8(uri);

      let overlapMs = 0;
      if (!BOUNDARY_SENT && COUNTDOWN_REMAINING_MS > 0) {
        if (COUNTDOWN_REMAINING_MS <= FRAME_MS) {
          overlapMs = FRAME_MS - COUNTDOWN_REMAINING_MS;
          BOUNDARY_SENT = true;
        }
        COUNTDOWN_REMAINING_MS = Math.max(0, COUNTDOWN_REMAINING_MS - FRAME_MS);
      }

      RESEND_BUFFER_PUT(FRAME_NO, {
        header: { FRAME_DURATION_IN_MS: FRAME_MS, COUNTDOWN_OVERLAP_MS: overlapMs },
        bytes: audioBytes,
      });

      await SEND_FRAME_PAIR({
        recordingId: RECORDING_ID,
        frameNo: FRAME_NO,
        frameMs: FRAME_MS,
        overlapMs,
        bytes: audioBytes,
      });

      try { await FileSystem.deleteAsync(uri, { idempotent: true }); } catch {}

      if (FRAME_NO < 0 && BOUNDARY_SENT) FRAME_NO = 0;
      else FRAME_NO += 1;
    } catch (e) {
      LOG('Streaming loop error', e?.message || e);
    }
  }

  await REFRESH_LOOP_WHILE_RECORDING();
  LOOP_TIMER = setInterval(REFRESH_LOOP_WHILE_RECORDING, FRAME_MS + SEND_SLACK_MS);
}

export async function STOP_STREAMING_WS() {
  console.log(`Start function CLIENT_AUDIO_STREAM_MASTER.STOP_STREAMING_WS`);
  if (!STREAMING) return;
  STREAMING = false;

  if (LOOP_TIMER) clearInterval(LOOP_TIMER);
  LOOP_TIMER = null;

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
}
