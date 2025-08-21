// CLIENT_APP_LOGGER.js
import * as Application from 'expo-application';
import * as FileSystem from 'expo-file-system';
import { Platform } from 'react-native';
import { CLIENT_APP_VARIABLES } from './CLIENT_APP_VARIABLES';

let MOBILE_DEVICE_ID_CACHE = null;
async function getDeviceId() {
  if (MOBILE_DEVICE_ID_CACHE) return MOBILE_DEVICE_ID_CACHE;
  try {
    if (Platform.OS === 'android') {
      MOBILE_DEVICE_ID_CACHE = Application.androidId ?? 'android-unknown';
    } else if (Platform.OS === 'ios') {
      MOBILE_DEVICE_ID_CACHE = Application.getIosIdForVendorAsync
        ? await Application.getIosIdForVendorAsync()
        : 'ios-unknown';
    } else {
      MOBILE_DEVICE_ID_CACHE = `${Platform.OS}-unknown`;
    }
  } catch {
    MOBILE_DEVICE_ID_CACHE = `${Platform.OS}-unknown`;
  }
  return MOBILE_DEVICE_ID_CACHE;
}

const QUEUE = [];
let timer = null;
const BATCH_MS = 1500;
const MAX_BATCH = 50;
const LOGFILE = FileSystem.documentDirectory + 'client.log';

function nowISO() { return new Date().toISOString(); }
function safeJSON(v) { try { return JSON.parse(JSON.stringify(v)); } catch { return '[unserializable]'; } }

async function writeLocalLog(line) {
  try {
    await FileSystem.writeAsStringAsync(LOGFILE, line + '\n', {
      encoding: FileSystem.EncodingType.UTF8,
      append: true
    });
  } catch {}
}

async function flush() {
  if (QUEUE.length === 0) return;
  const LOG_ENTRY = QUEUE.splice(0, QUEUE.length);
  try {
    await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CLIENT_LOG`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ LOG_ENTRY })
    });
  } catch {
    for (const e of LOG_ENTRY) await writeLocalLog(JSON.stringify(e));
  }
}

// ---------- PUBLIC API -----------------------------------------------------

/**
 * Log an individual STEP/INFO/ERROR line.
 */
export async function CLIENT_DB_LOG_STEPS(
  level,                  // "INFO" | "ERROR" | "WARN" | ...
  reactFileName,          // e.g., "SCREEN_MAIN.js"
  reactFunctionOrStep,    // e.g., "onPressStart" or "STEP:LOADED_CACHE"
  logText,                // message or error text
  opts = {}               // optional payloads
) {
  const deviceId = await getDeviceId();
  QUEUE.push({
    MOBILE_DEVICE_ID: deviceId,
    MOBILE_DEVICE_PLATFORM: Platform.OS,
    DT_LOG_ENTRY: nowISO(),
    REACT_FILE_NAME: reactFileName ?? 'UNKNOWN_FILE',
    REACT_FUNCTION_NAME: opts.reactFunctionName ?? null,
    REACT_STEP_NAME: reactFunctionOrStep ?? null,
    START_END_ERROR_OR_STEP: level === 'ERROR' ? 'ERROR' : 'STEP', // or keep literal level if you prefer
    LOG_MSG: String(logText ?? ''),
    CLIENT_APP_VARIABLES_JSON: safeJSON(opts.clientAppVars ?? CLIENT_APP_VARIABLES),
    CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME_JSON: safeJSON(opts.wsAudioFrame ?? null),
    CLIENT_DB_LOG_WEBSOCKET_CONNECTION_JSON: safeJSON(opts.wsConnection ?? null),
    CLIENT_DB_LOG_WEBSOCKET_MESSAGE_JSON: safeJSON(opts.wsMessage ?? null),
    LOCAL_VARIABLES_JSON: safeJSON(opts.localVars ?? null)
  });

  if (QUEUE.length >= MAX_BATCH) flush();
  if (!timer) timer = setInterval(flush, BATCH_MS);
}

/**
 * Wrap a function to auto-log START / END / ERROR with your schema.
 */
export function CLIENT_DB_LOG_FUNCTIONS(tag, fn, options = {}) {
  const {
    reactFileName = 'UNKNOWN_FILE',
    reactFunctionName = fn?.name || 'anonymous',
    localVarsProvider,
    providers = {}
  } = options;

  return async (...args) => {
    const deviceId = await getDeviceId();

    // START
    QUEUE.push({
      MOBILE_DEVICE_ID: deviceId,
      MOBILE_DEVICE_PLATFORM: Platform.OS,
      DT_LOG_ENTRY: nowISO(),
      REACT_FILE_NAME: reactFileName,
      REACT_FUNCTION_NAME: reactFunctionName,
      REACT_STEP_NAME: `${tag}:${reactFunctionName}`,
      START_END_ERROR_OR_STEP: 'START',
      LOG_MSG: 'START',
      CLIENT_APP_VARIABLES_JSON: safeJSON(providers.clientAppVars?.() ?? CLIENT_APP_VARIABLES),
      CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME_JSON: safeJSON(providers.wsAudioFrame?.() ?? null),
      CLIENT_DB_LOG_WEBSOCKET_CONNECTION_JSON: safeJSON(providers.wsConnection?.() ?? null),
      CLIENT_DB_LOG_WEBSOCKET_MESSAGE_JSON: safeJSON(providers.wsMessage?.() ?? null),
      LOCAL_VARIABLES_JSON: safeJSON(localVarsProvider ? localVarsProvider(...args) : { args })
    });

    const started = Date.now();
    try {
      const result = await fn(...args);

      // END
      QUEUE.push({
        MOBILE_DEVICE_ID: deviceId,
        MOBILE_DEVICE_PLATFORM: Platform.OS,
        DT_LOG_ENTRY: nowISO(),
        REACT_FILE_NAME: reactFileName,
        REACT_FUNCTION_NAME: reactFunctionName,
        REACT_STEP_NAME: `${tag}:${reactFunctionName}`,
        START_END_ERROR_OR_STEP: 'END',
        LOG_MSG: `END (${Date.now() - started} ms)`,
        CLIENT_APP_VARIABLES_JSON: safeJSON(providers.clientAppVars?.() ?? CLIENT_APP_VARIABLES),
        CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME_JSON: safeJSON(providers.wsAudioFrame?.() ?? null),
        CLIENT_DB_LOG_WEBSOCKET_CONNECTION_JSON: safeJSON(providers.wsConnection?.() ?? null),
        CLIENT_DB_LOG_WEBSOCKET_MESSAGE_JSON: safeJSON(providers.wsMessage?.() ?? null),
        LOCAL_VARIABLES_JSON: null
      });

      if (QUEUE.length >= MAX_BATCH) flush();
      if (!timer) timer = setInterval(flush, BATCH_MS);

      return result;
    } catch (err) {
      // ERROR
      QUEUE.push({
        MOBILE_DEVICE_ID: deviceId,
        MOBILE_DEVICE_PLATFORM: Platform.OS,
        DT_LOG_ENTRY: nowISO(),
        REACT_FILE_NAME: reactFileName,
        REACT_FUNCTION_NAME: reactFunctionName,
        REACT_STEP_NAME: `${tag}:${reactFunctionName}`,
        START_END_ERROR_OR_STEP: 'ERROR',
        LOG_MSG: String(err?.message || err),
        CLIENT_APP_VARIABLES_JSON: safeJSON(providers.clientAppVars?.() ?? CLIENT_APP_VARIABLES),
        CLIENT_DB_LOG_WEBSOCKET_AUDIO_FRAME_JSON: safeJSON(providers.wsAudioFrame?.() ?? null),
        CLIENT_DB_LOG_WEBSOCKET_CONNECTION_JSON: safeJSON(providers.wsConnection?.() ?? null),
        CLIENT_DB_LOG_WEBSOCKET_MESSAGE_JSON: safeJSON(providers.wsMessage?.() ?? null),
        LOCAL_VARIABLES_JSON: safeJSON({ stack: String(err?.stack || '') })
      });

      if (QUEUE.length >= MAX_BATCH) flush();
      if (!timer) timer = setInterval(flush, BATCH_MS);

      throw err;
    }
  };
}

export async function CLIENT_DB_LOG_FLUSH_NOW() { await flush(); }
