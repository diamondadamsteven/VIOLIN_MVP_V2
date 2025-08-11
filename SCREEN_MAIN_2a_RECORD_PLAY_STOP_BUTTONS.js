// SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.js
import { FontAwesome } from '@expo/vector-icons';
import { DeviceEventEmitter, StyleSheet, TouchableOpacity, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';
import { START_STREAMING_WS, STOP_STREAMING_WS } from './CLIENT_AUDIO_STREAM_MASTER';

// Event names other components can listen to (notes, scores, conductor UI, etc.)
export const EVT_NOTES_UPDATED = 'EVT_NOTES_UPDATED';
export const EVT_SCORES_UPDATED = 'EVT_SCORES_UPDATED';
export const EVT_CONDUCTOR_UPDATED = 'EVT_CONDUCTOR_UPDATED';

// ─────────────────────────────────────────────────────────────
// Simple procedural flags in app vars (no useState)
CLIENT_APP_VARIABLES._IS_PLAYING = false;
CLIENT_APP_VARIABLES._IS_RECORDING = false;
CLIENT_APP_VARIABLES._IS_PAUSED = false;
// ─────────────────────────────────────────────────────────────

function LOG(msg, obj) {
  const prefix = 'SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS';
  if (obj !== undefined) console.log(`${prefix} - ${msg}`, obj);
  else console.log(`${prefix} - ${msg}`);
}

// Generic SP caller
async function CALL_SP(SP_NAME, PARAMS) {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.CALL_SP`);
  console.log(`Calling sp ${SP_NAME} ${JSON.stringify(PARAMS)}`);
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
  const json = await resp.json().catch(() => ({}));
  return json;
}

// Record start → call P_CLIENT_RECORD_START, populate app vars, kick countdown & WS
async function RECORD_BUTTON_TAPPED_HANDLER() {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.RECORD_BUTTON_TAPPED_HANDLER`);
  LOG('Record button tapped');

  CLIENT_APP_VARIABLES._IS_RECORDING = true;
  CLIENT_APP_VARIABLES._IS_PLAYING = false;
  CLIENT_APP_VARIABLES._IS_PAUSED = false;

  // 1) Call P_CLIENT_RECORD_START
  const params = {
    VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
    SONG_ID: CLIENT_APP_VARIABLES.SONG_ID,
  };
  const rs = await CALL_SP('P_CLIENT_RECORD_START', params);

  // Expect a single-record result with fields per spec:
  const row =
    rs?.result?.[0] ||
    rs?.rows?.[0] ||
    rs?.[0] ||
    null;

  if (!row) {
    throw new Error('P_CLIENT_RECORD_START returned no rows.');
  }

  // Assign to app variables
  CLIENT_APP_VARIABLES.SONG_ID = row.SONG_ID ?? CLIENT_APP_VARIABLES.SONG_ID;
  CLIENT_APP_VARIABLES.RECORDING_ID = row.RECORDING_ID;
  CLIENT_APP_VARIABLES.COMPOSE_CHUNK_MINIMUM_DURATION_IN_MS = row.COMPOSE_CHUNK_MINIMUM_DURATION_IN_MS;
  CLIENT_APP_VARIABLES.COUNTDOWN_BEATS = row.COUNTDOWN_BEATS;
  CLIENT_APP_VARIABLES.CONDUCTOR_MESSAGE_TEXT = row.CONDUCTOR_MESSAGE_TEXT;
  CLIENT_APP_VARIABLES.CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS = row.CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS;
  CLIENT_APP_VARIABLES.CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL = row.CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL;
  CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME = row.AUDIO_STREAM_FILE_NAME;

  LOG('P_CLIENT_RECORD_START → app vars updated', {
    RECORDING_ID: CLIENT_APP_VARIABLES.RECORDING_ID,
    COUNTDOWN_BEATS: CLIENT_APP_VARIABLES.COUNTDOWN_BEATS,
    AUDIO_STREAM_FILE_NAME: CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME,
  });

  // 2) Start WebSocket streaming (countdown + frames)
  const bpm = CLIENT_APP_VARIABLES.BPM || 60;
  await START_STREAMING_WS({
    countdownBeats: CLIENT_APP_VARIABLES.COUNTDOWN_BEATS || 0,
    bpm,
  });

  // 3) Kick the “while-recording” refresh loop (notes/colors/scores/etc.)
  REFRESH_LOOP_WHILE_RECORDING();
}

// Stop → call P_CLIENT_RECORD_END, stop WS
async function STOP_BUTTON_TAPPED_HANDLER() {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.STOP_BUTTON_TAPPED_HANDLER`);
  LOG('Stop button tapped');

  CLIENT_APP_VARIABLES._IS_PAUSED = false;
  CLIENT_APP_VARIABLES._IS_PLAYING = false;
  CLIENT_APP_VARIABLES._IS_RECORDING = false;

  // Stop streaming first (flush/STOP to server)
  try {
    await STOP_STREAMING_WS();
  } catch (e) {
    LOG('STOP_STREAMING_WS error', e?.message || e);
  }

  // Tell backend recording ended
  const params = {
    RECORDING_ID: CLIENT_APP_VARIABLES.RECORDING_ID,
  };
  await CALL_SP('P_CLIENT_RECORD_END', params);

  // Clear any chunk bounds in app vars (as per spec)
  CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO = null;
  CLIENT_APP_VARIABLES.END_AUDIO_CHUNK_NO = null;

  LOG('Recording stopped and P_CLIENT_RECORD_END called');
}

// Play tapped → (not implementing playback here; just log and allow parent to handle)
function PLAY_BUTTON_TAPPED_HANDLER() {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.PLAY_BUTTON_TAPPED_HANDLER`);
  LOG('Play button tapped');
  CLIENT_APP_VARIABLES._IS_PLAYING = true;
  CLIENT_APP_VARIABLES._IS_RECORDING = false;
  CLIENT_APP_VARIABLES._IS_PAUSED = false;

  // Let the parent screen do its UI things (batons, face, etc.) via props callback
  // This component stays transport-only.
}

// Pause tapped (optional state flag only)
function PAUSE_BUTTON_TAPPED_HANDLER() {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.PAUSE_BUTTON_TAPPED_HANDLER`);
  LOG('Pause button tapped');
  CLIENT_APP_VARIABLES._IS_PAUSED = true;
}

// ─────────────────────────────────────────────────────────────
// While-recording refresh loop per spec:
// Repeatedly:
//  - P_CLIENT_SONG_AUDIO_CHUNK_PROCESSED_GET
//      If YN_STOP_RECORDING = 'Y' → trigger stop
//      Then set YN_REFRESH_NEXT_AUDIO_CHUNK = null (via SP below)
//  - P_CLIENT_SONG_NOTES_GET → emit EVT_NOTES_UPDATED
//  - (If YN_SHOW_ADVANCED = 'Y') P_CLIENT_RPT_AUDIO_DETAIL_GET
//  - P_CLIENT_SONG_SCORE_GET → emit EVT_SCORES_UPDATED
//  - P_CLIENT_SONG_ANIMATION_GET → emit EVT_CONDUCTOR_UPDATED
//  - Set YN_REFRESH_NEXT_AUDIO_CHUNK = 'Y'
// Notes:
//  - This is a light v1 loop. You can tune cadence (e.g., 200–300ms).
// ─────────────────────────────────────────────────────────────
let _refreshTimer = null;
const REFRESH_CADENCE_MS = 300;

async function REFRESH_LOOP_ITERATION() {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.REFRESH_LOOP_ITERATION`);
  if (!CLIENT_APP_VARIABLES._IS_RECORDING) return;

  const RECORDING_ID = CLIENT_APP_VARIABLES.RECORDING_ID;

  try {
    // A) Which chunk/state changed?
    const a = await CALL_SP('P_CLIENT_SONG_AUDIO_CHUNK_PROCESSED_GET', {
      RECORDING_ID,
    });

    const rowA = a?.result?.[0] || a?.rows?.[0] || a?.[0] || {};
    if (rowA.YN_STOP_RECORDING === 'Y') {
      // Trigger stop
      await STOP_BUTTON_TAPPED_HANDLER();
      return;
    }

    // Clear YN_REFRESH_NEXT_AUDIO_CHUNK
    await CALL_SP('P_CLIENT_SONG_REFRESH_FLAG_UPD', {
      RECORDING_ID,
      YN_REFRESH_NEXT_AUDIO_CHUNK: null,
    });

    // B) Notes
    const notesRs = await CALL_SP('P_CLIENT_SONG_NOTES_GET', {
      RECORDING_ID,
      BREAKDOWN_NAME: CLIENT_APP_VARIABLES.BREAKDOWN_NAME || 'OVERALL',
    });
    const notes = notesRs?.result || notesRs?.rows || [];
    CLIENT_APP_VARIABLES._LAST_NOTES = notes;
    DeviceEventEmitter.emit(EVT_NOTES_UPDATED, { notes });

    // C) Advanced (optional)
    if (CLIENT_APP_VARIABLES.YN_SHOW_ADVANCED === 'Y') {
      await CALL_SP('P_CLIENT_RPT_AUDIO_DETAIL_GET', { RECORDING_ID });
    }

    // D) Scores
    const scoresRs = await CALL_SP('P_CLIENT_SONG_SCORE_GET', { RECORDING_ID });
    const scores = scoresRs?.result || scoresRs?.rows || [];
    CLIENT_APP_VARIABLES._LAST_SCORES = scores;
    DeviceEventEmitter.emit(EVT_SCORES_UPDATED, { scores });

    // E) Conductor animation
    const animRs = await CALL_SP('P_CLIENT_SONG_ANIMATION_GET', { RECORDING_ID });
    const animation = animRs?.result?.[0] || animRs?.rows?.[0] || null;
    CLIENT_APP_VARIABLES._LAST_CONDUCTOR = animation;
    DeviceEventEmitter.emit(EVT_CONDUCTOR_UPDATED, { animation });

    // F) Set refresh flag back to 'Y'
    await CALL_SP('P_CLIENT_SONG_REFRESH_FLAG_UPD', {
      RECORDING_ID,
      YN_REFRESH_NEXT_AUDIO_CHUNK: 'Y',
    });
  } catch (err) {
    LOG('REFRESH_LOOP_ITERATION error', err?.message || err);
  }
}

export function REFRESH_LOOP_WHILE_RECORDING() {
  console.log(`Start function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.REFRESH_LOOP_WHILE_RECORDING`);
  if (_refreshTimer) clearInterval(_refreshTimer);
  _refreshTimer = setInterval(() => {
    if (!CLIENT_APP_VARIABLES._IS_RECORDING) {
      clearInterval(_refreshTimer);
      _refreshTimer = null;
      return;
    }
    REFRESH_LOOP_ITERATION();
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
