// SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.js
import { FontAwesome } from '@expo/vector-icons';
import { DeviceEventEmitter, StyleSheet, TouchableOpacity, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';
import { START_STREAMING_WS, STOP_STREAMING_WS } from './CLIENT_AUDIO_STREAM_MASTER';

// Event names other components can listen to (notes, scores, conductor UI, etc.)
export const EVT_NOTES_UPDATED = 'EVT_NOTES_UPDATED';
export const EVT_SCORES_UPDATED = 'EVT_SCORES_UPDATED';
export const EVT_CONDUCTOR_UPDATED = 'EVT_CONDUCTOR_UPDATED';
// Parent (SCREEN_MAIN.js) listens to this to refresh panels during recording
export const EVT_PANELS_REFRESH_REQUESTED = 'EVT_PANELS_REFRESH_REQUESTED';

// ─────────────────────────────────────────────────────────────
// Simple procedural flags in app vars (no useState)
CLIENT_APP_VARIABLES._IS_PLAYING = false;
CLIENT_APP_VARIABLES._IS_RECORDING = false;
CLIENT_APP_VARIABLES._IS_PAUSED = false;
// Track last chunk we reacted to
let L_START_AUDIO_CHUNK_NO = 0;
// ─────────────────────────────────────────────────────────────

function LOG(msg, obj) {
  const prefix = 'SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS';
  if (obj !== undefined) console.log(`${prefix} - ${msg}`, obj);
  else console.log(`${prefix} - ${msg}`);
}

// Generic SP caller
async function CALL_SP(SP_NAME, PARAMS) {
  LOG('CALL_SP', { SP_NAME, PARAMS });
  const url = `${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`;
  const body = { SP_NAME, PARAMS };
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`SP ${SP_NAME} failed: ${resp.status} ${text}`);
  }
  return resp.json().catch(() => ({}));
}

// Record start → call P_CLIENT_RECORD_START, populate app vars, kick countdown & WS
async function RECORD_BUTTON_TAPPED_HANDLER() {
  LOG('RECORD_BUTTON_TAPPED_HANDLER');

  CLIENT_APP_VARIABLES._IS_RECORDING = true;
  CLIENT_APP_VARIABLES._IS_PLAYING = false;
  CLIENT_APP_VARIABLES._IS_PAUSED = false;
  L_START_AUDIO_CHUNK_NO = 0;

  const request = {
    SP_NAME: 'P_CLIENT_RECORD_START',
    PARAMS: {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      SONG_ID: CLIENT_APP_VARIABLES.SONG_ID,
      COMPOSE_SONG_NAME: CLIENT_APP_VARIABLES.SONG_NAME,
      COMPOSE_TIME_SIGNATURE: CLIENT_APP_VARIABLES.TIME_SIGNATURE,
      COMPOSE_PARAMETER_VALUE_FASTEST_NOTE_IN_BEATS: CLIENT_APP_VARIABLES.FASTEST_NOTE_IN_BEATS,
      COMPOSE_PARAMETER_VALUE_DOUBLE_STOPS: CLIENT_APP_VARIABLES.YN_HAS_DOUBLE_STOPS,
      COMPOSE_PARAMETER_VALUE_HIGH_NOTES: CLIENT_APP_VARIABLES.YN_HAS_HIGH_NOTES,
      PARAMETER_VALUE_BPM: CLIENT_APP_VARIABLES.BPM,
      PARAMETER_VALUE_TUNING: CLIENT_APP_VARIABLES.TUNING,
      PLAY_PARAMETER_VALUE_GOAL_TARGET: CLIENT_APP_VARIABLES.GOAL_TARGET,
      PLAY_GOAL_TARGET_RECORDING_ID: CLIENT_APP_VARIABLES.GOAL_TARGET_RECORDING_ID,
      COMPOSE_PLAY_OR_PRACTICE: CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE,
    },
  };

  const startResp = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });

  const startData = await startResp.json();
  LOG('P_CLIENT_RECORD_START response', startData);

  // Assign to app variables
  CLIENT_APP_VARIABLES.SONG_ID = startData?.RESULT?.SONG_ID;
  CLIENT_APP_VARIABLES.RECORDING_ID = startData?.RESULT?.RECORDING_ID;
  CLIENT_APP_VARIABLES.COMPOSE_CHUNK_MINIMUM_DURATION_IN_MS =
    startData?.RESULT?.COMPOSE_CHUNK_MINIMUM_DURATION_IN_MS;
  CLIENT_APP_VARIABLES.COUNTDOWN_BEATS = startData?.RESULT?.COUNTDOWN_BEATS;
  CLIENT_APP_VARIABLES.CONDUCTOR_MESSAGE_TEXT = startData?.RESULT?.CONDUCTOR_MESSAGE_TEXT;
  CLIENT_APP_VARIABLES.CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS =
    startData?.RESULT?.CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS;
  CLIENT_APP_VARIABLES.CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL =
    startData?.RESULT?.CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL;
  CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME = startData?.RESULT?.AUDIO_STREAM_FILE_NAME;

  // Start WebSocket streaming (countdown + frames)
  const bpm = CLIENT_APP_VARIABLES.BPM;
  await START_STREAMING_WS({ countdownBeats: CLIENT_APP_VARIABLES.COUNTDOWN_BEATS || 0, bpm });

  // Kick the while-recording refresh loop
  REFRESH_LOOP_WHILE_RECORDING();
}

// Stop → call P_CLIENT_RECORD_END, stop WS
async function STOP_BUTTON_TAPPED_HANDLER() {
  LOG('STOP_BUTTON_TAPPED_HANDLER');

  CLIENT_APP_VARIABLES._IS_PAUSED = false;
  CLIENT_APP_VARIABLES._IS_PLAYING = false;
  CLIENT_APP_VARIABLES._IS_RECORDING = false;

  try {
    await STOP_STREAMING_WS();
  } catch (e) {
    LOG('STOP_STREAMING_WS error', e?.message || e);
  }

  await CALL_SP('P_CLIENT_RECORD_END', { RECORDING_ID: CLIENT_APP_VARIABLES.RECORDING_ID });

  CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO = null;
  CLIENT_APP_VARIABLES.END_AUDIO_CHUNK_NO = null;

  LOG('Recording stopped and P_CLIENT_RECORD_END called');
}

// Play tapped (transport-only; UI handled by parent)
function PLAY_BUTTON_TAPPED_HANDLER() {
  L_START_AUDIO_CHUNK_NO = 0;
  LOG('PLAY_BUTTON_TAPPED_HANDLER');
  CLIENT_APP_VARIABLES._IS_PLAYING = true;
  CLIENT_APP_VARIABLES._IS_RECORDING = false;
  CLIENT_APP_VARIABLES._IS_PAUSED = false;
}

// Pause tapped
function PAUSE_BUTTON_TAPPED_HANDLER() {
  LOG('PAUSE_BUTTON_TAPPED_HANDLER');
  CLIENT_APP_VARIABLES._IS_PAUSED = true;
}

// ─────────────────────────────────────────────────────────────
// While-recording refresh loop
// ─────────────────────────────────────────────────────────────
let _refreshTimer = null;
const REFRESH_CADENCE_MS = 300;

async function REFRESH_LOOP_ITERATION() {
  if (!CLIENT_APP_VARIABLES._IS_RECORDING) return;

  try {
    const payload = {
      SP_NAME: 'P_CLIENT_SONG_AUDIO_CHUNK_PROCESSED_GET',
      PARAMS: {
        VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
        RECORDING_ID: CLIENT_APP_VARIABLES.RECORDING_ID,
      },
    };

    const resp = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    const data = await resp.json();
    LOG('P_CLIENT_SONG_AUDIO_CHUNK_PROCESSED_GET', data);

    CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO = data?.RESULT?.START_AUDIO_CHUNK_NO;
    CLIENT_APP_VARIABLES.END_AUDIO_CHUNK_NO = data?.RESULT?.END_AUDIO_CHUNK_NO;
    CLIENT_APP_VARIABLES.YN_STOP_RECORDING = data?.RESULT?.YN_STOP_RECORDING;
    CLIENT_APP_VARIABLES.YN_STOP_CLIENT_REFRESH_LOOP = data?.RESULT?.YN_STOP_CLIENT_REFRESH_LOOP;

    // New chunk processed? Ask panels to refresh via event.
    if (
      typeof CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO === 'number' &&
      CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO > L_START_AUDIO_CHUNK_NO
    ) {
      DeviceEventEmitter.emit(EVT_PANELS_REFRESH_REQUESTED);
      L_START_AUDIO_CHUNK_NO = CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO;
      return;
    }
  } catch (err) {
    LOG('REFRESH_LOOP_ITERATION error', err?.message || err);
  }
}

export function REFRESH_LOOP_WHILE_RECORDING() {
  LOG('REFRESH_LOOP_WHILE_RECORDING');
  if (_refreshTimer) clearInterval(_refreshTimer);

  _refreshTimer = setInterval(async () => {
    // Stop the loop if recording has ended
    if (!CLIENT_APP_VARIABLES._IS_RECORDING) {
      clearInterval(_refreshTimer);
      _refreshTimer = null;
      return;
    }

    await REFRESH_LOOP_ITERATION();

    // Auto-stop if backend says to stop
    if (
      CLIENT_APP_VARIABLES.YN_STOP_RECORDING === 'Y'  &&
      (CLIENT_APP_VARIABLES._IS_RECORDING || CLIENT_APP_VARIABLES._IS_PLAYING)
    ) {
      await STOP_BUTTON_TAPPED_HANDLER();
    }

    // Or stop just the client loop if asked
    if (CLIENT_APP_VARIABLES.YN_STOP_CLIENT_REFRESH_LOOP === 'Y') {
      clearInterval(_refreshTimer);
      _refreshTimer = null;
      return;
    }
  }, REFRESH_CADENCE_MS);
}

// UI component
export default function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS({
  USER_EVENT_PLAY_BUTTON_TAPPED,
  USER_EVENT_RECORD_BUTTON_TAPPED,
  USER_EVENT_PAUSE_BUTTON_TAPPED,
  USER_EVENT_STOP_BUTTON_TAPPED,
}) {
  return (
    <View style={STYLES.container}>
      {/* Idle state: show record and play */}
      {!CLIENT_APP_VARIABLES._IS_PLAYING &&
        !CLIENT_APP_VARIABLES._IS_RECORDING &&
        !CLIENT_APP_VARIABLES._IS_PAUSED && (
          <>
            {!CLIENT_APP_VARIABLES.RECORDING_ID && (
              <TouchableOpacity
                onPress={async () => {
                  await RECORD_BUTTON_TAPPED_HANDLER();
                  USER_EVENT_RECORD_BUTTON_TAPPED?.();
                }}
                style={STYLES.circleButton}
              >
                <FontAwesome name="circle" size={32} color="red" />
              </TouchableOpacity>
            )}
            <TouchableOpacity
              onPress={() => {
                PLAY_BUTTON_TAPPED_HANDLER();
                USER_EVENT_PLAY_BUTTON_TAPPED?.();
              }}
              style={STYLES.circleButton}
            >
              <FontAwesome name="play-circle" size={32} color="black" />
            </TouchableOpacity>
          </>
        )}

      {/* Active (playing or recording): show pause + stop */}
      {(CLIENT_APP_VARIABLES._IS_PLAYING || CLIENT_APP_VARIABLES._IS_RECORDING) &&
        !CLIENT_APP_VARIABLES._IS_PAUSED && (
          <>
            <TouchableOpacity
              onPress={() => {
                PAUSE_BUTTON_TAPPED_HANDLER();
                USER_EVENT_PAUSE_BUTTON_TAPPED?.();
              }}
              style={STYLES.circleButton}
            >
              <FontAwesome name="pause-circle" size={32} color="black" />
            </TouchableOpacity>
            <TouchableOpacity
              onPress={async () => {
                await STOP_BUTTON_TAPPED_HANDLER();
                USER_EVENT_STOP_BUTTON_TAPPED?.();
              }}
              style={STYLES.circleButton}
            >
              <FontAwesome name="stop-circle" size={32} color="black" />
            </TouchableOpacity>
          </>
        )}

      {/* Paused state: show record (if no take yet) + play */}
      {CLIENT_APP_VARIABLES._IS_PAUSED && (
        <>
          {!CLIENT_APP_VARIABLES.RECORDING_ID && (
            <TouchableOpacity
              onPress={async () => {
                await RECORD_BUTTON_TAPPED_HANDLER();
                USER_EVENT_RECORD_BUTTON_TAPPED?.();
              }}
              style={STYLES.circleButton}
            >
              <FontAwesome name="circle" size={32} color="red" />
            </TouchableOpacity>
          )}
          <TouchableOpacity
            onPress={() => {
              PLAY_BUTTON_TAPPED_HANDLER();
              USER_EVENT_PLAY_BUTTON_TAPPED?.();
            }}
            style={STYLES.circleButton}
          >
            <FontAwesome name="play-circle" size={32} color="black" />
          </TouchableOpacity>
        </>
      )}
    </View>
  );
}

const STYLES = StyleSheet.create({
  container: {
    flexDirection: 'row',
    gap: 10,
    marginVertical: 4,
    justifyContent: 'flex-start',
    alignItems: 'flex-start',
  },
  circleButton: {
    padding: 4,
  },
});
