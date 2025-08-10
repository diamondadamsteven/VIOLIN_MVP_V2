// CLIENT_AUDIO_STREAM_MASTER_WS.js
import { Audio } from 'expo-av';
import * as FileSystem from 'expo-file-system';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

const SERVER_HTTP = 'http://<your-ip>:7070';
const SERVER_WS   = 'ws://<your-ip>:7070/ws/stream';

const CHUNK_MS = 250;     // try 250–300ms
const RESEND_BUFFER_SIZE = 128;

let WS = null;
let LOOP_TIMER = null;

let FRAME_NO = -3;
let COUNTDOWN_REMAINING_MS = 0;
let BOUNDARY_SENT = false;

// Simple ring buffer using Map; drop oldest when > RESEND_BUFFER_SIZE
const RESEND_BUFFER = new Map(); // FRAME_NO -> { bytes: Uint8Array, header: object }
function bufferPut(frameNo, entry) {
  RESEND_BUFFER.set(frameNo, entry);
  if (RESEND_BUFFER.size > RESEND_BUFFER_SIZE) {
    const oldestKey = RESEND_BUFFER.keys().next().value;
    RESEND_BUFFER.delete(oldestKey);
  }
}
function bufferGet(frameNo) {
  return RESEND_BUFFER.get(frameNo);
}

function encodeMessage(headerObj, audioBytes) {
  const headerStr = JSON.stringify(headerObj);
  const headerUtf8 = new TextEncoder().encode(headerStr);
  const headerLen = headerUtf8.length;

  const buf = new Uint8Array(2 + headerLen + audioBytes.length);
  buf[0] = (headerLen >> 8) & 0xff;
  buf[1] = headerLen & 0xff;
  buf.set(headerUtf8, 2);
  buf.set(audioBytes, 2 + headerLen);
  return buf;
}

async function recordMicroChunk(ms) {
  const REC = new Audio.Recording();
  await Audio.setAudioModeAsync({
    allowsRecordingIOS: true,
    playsInSilentModeIOS: true,
    interruptionModeIOS: Audio.INTERRUPTION_MODE_IOS_DO_NOT_MIX,
    shouldDuckAndroid: true,
    interruptionModeAndroid: Audio.INTERRUPTION_MODE_ANDROID_DO_NOT_MIX,
  });
  await REC.prepareToRecordAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
  await REC.startAsync();
  await new Promise((r) => setTimeout(r, ms));
  await REC.stopAndUnloadAsync();
  const uri = REC.getURI();
  return uri;
}

async function readFileAsUint8(uri) {
  const base64 = await FileSystem.readAsStringAsync(uri, { encoding: FileSystem.EncodingType.Base64 });
  const bin = atob(base64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

async function sendFrame(frameNo, header, bytes) {
  if (!WS || WS.readyState !== 1) return;
  const payload = encodeMessage(header, bytes);
  WS.send(payload);
}

export async function START_STREAMING_WS({ countdownBeats, bpm }) {
  // STEP 1 via HTTP
  {
    const fd = new FormData();
    fd.append('RECORDING_ID', String(CLIENT_APP_VARIABLES.RECORDING_ID));
    fd.append('STREAMED_CHUNK_DURATION_IN_MS', String(CHUNK_MS));
    await fetch(`${SERVER_HTTP}/STEP_1_START_RECORDING`, {
      method: 'POST',
      headers: { 'Content-Type': 'multipart/form-data' },
      body: fd,
    });
  }

  // Open WS
  WS = new WebSocket(`${SERVER_WS}/${CLIENT_APP_VARIABLES.RECORDING_ID}`);
  WS.binaryType = 'arraybuffer';

  // ACK handler: resend any missing frames (if still buffered)
  WS.onmessage = (evt) => {
    try {
      const ack = JSON.parse(typeof evt.data === 'string' ? evt.data : new TextDecoder().decode(evt.data));
      const missing = ack?.MISSING_FRAMES || [];
      for (const m of missing) {
        const entry = bufferGet(m);
        if (entry) {
          // resend *exactly the same header+bytes*
          sendFrame(m, entry.header, entry.bytes);
        }
      }
    } catch {}
  };

  // Initialize countdown
  const MS_PER_BEAT = 60000 / bpm;
  COUNTDOWN_REMAINING_MS = Math.round(countdownBeats * MS_PER_BEAT);
  FRAME_NO = -Math.ceil(COUNTDOWN_REMAINING_MS / CHUNK_MS);
  BOUNDARY_SENT = false;

  // Wait for WS open
  await new Promise((res) => {
    if (WS.readyState === 1) return res();
    const onOpen = () => { WS.removeEventListener('open', onOpen); res(); };
    WS.addEventListener('open', onOpen);
  });

  const LOOP = async () => {
    if (!WS || WS.readyState !== 1) return;

    const uri = await recordMicroChunk(CHUNK_MS);
    const audioBytes = await readFileAsUint8(uri);

    let header = { FRAME_NO };
    if (!BOUNDARY_SENT && COUNTDOWN_REMAINING_MS > 0) {
      if (COUNTDOWN_REMAINING_MS <= CHUNK_MS) {
        header.COUNTDOWN_ZERO_IN_THIS_CHUNK = 'Y';
        header.COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK = COUNTDOWN_REMAINING_MS;
        BOUNDARY_SENT = true;
      }
      COUNTDOWN_REMAINING_MS = Math.max(0, COUNTDOWN_REMAINING_MS - CHUNK_MS);
    }

    // buffer before send (for possible resend)
    bufferPut(FRAME_NO, { header, bytes: audioBytes });

    await sendFrame(FRAME_NO, header, audioBytes);

    try { await FileSystem.deleteAsync(uri, { idempotent: true }); } catch {}

    FRAME_NO = (FRAME_NO < 0 && BOUNDARY_SENT) ? 0 : FRAME_NO + 1;
  };

  await LOOP();
  LOOP_TIMER = setInterval(LOOP, CHUNK_MS + 40);
}

export async function STOP_STREAMING_WS() {
  if (LOOP_TIMER) clearInterval(LOOP_TIMER);
  LOOP_TIMER = null;

  if (WS) {
    try { WS.close(); } catch {}
    WS = null;
  }

  await fetch(`${SERVER_HTTP}/STEP_3_STOP_RECORDING/${CLIENT_APP_VARIABLES.RECORDING_ID}`, { method: 'POST' });
}
