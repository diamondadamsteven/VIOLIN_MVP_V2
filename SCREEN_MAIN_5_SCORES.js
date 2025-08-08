// SCREEN_MAIN_5_SCORES.js
import { forwardRef, useEffect, useImperativeHandle, useState } from 'react';
import { ScrollView, StyleSheet, Text, TouchableOpacity, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

const TAG = '🧮 SCREEN_MAIN_5_SCORES';

const SCREEN_MAIN_5_SCORES = forwardRef(({ USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED }, ref) => {
  const [RESULT_SET_P_CLIENT_SONG_SCORE_GET, SET_RESULT_SET_P_CLIENT_SONG_SCORE_GET] = useState([]);

  // ---- MOUNT / UNMOUNT LOGS -------------------------------------------------
  useEffect(() => {
    console.log(`${TAG} → MOUNT (props: has USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED=${!!USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED})`);
    return () => console.log(`${TAG} → UNMOUNT`);
  }, [USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED]);

  // ---- REFRESH (exposed to parent) ------------------------------------------
  const REFRESH = async () => {
    try {
      console.log(`${TAG} → REFRESH called with:`, {
        VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
        SONG_ID: CLIENT_APP_VARIABLES.SONG_ID,
        RECORDING_ID: CLIENT_APP_VARIABLES.RECORDING_ID,
        START_AUDIO_CHUNK_NO: CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO,
        BREAKDOWN_NAME: CLIENT_APP_VARIABLES.BREAKDOWN_NAME || 'OVERALL',
        YN_RECORDING_IN_PROGRESS: CLIENT_APP_VARIABLES.YN_RECORDING_IN_PROGRESS,
        BACKEND_URL: CLIENT_APP_VARIABLES.BACKEND_URL,
      });

      if (!CLIENT_APP_VARIABLES.BREAKDOWN_NAME) {
        CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
        console.log(`${TAG} → BREAKDOWN_NAME was null; defaulted to OVERALL`);
      }

      const URL = `${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`;
      const BODY = {
        SP_NAME: 'P_CLIENT_SONG_SCORE_GET',
        PARAMS: {
          VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
          SONG_ID: CLIENT_APP_VARIABLES.SONG_ID,
          RECORDING_ID: CLIENT_APP_VARIABLES.RECORDING_ID,
          //START_AUDIO_CHUNK_NO: CLIENT_APP_VARIABLES.START_AUDIO_CHUNK_NO,
          BREAKDOWN_NAME: CLIENT_APP_VARIABLES.BREAKDOWN_NAME,
          YN_RECORDING_IN_PROGRESS: CLIENT_APP_VARIABLES.YN_RECORDING_IN_PROGRESS,
        },
      };

      console.log(`${TAG} → Fetching scores from: ${URL}`);
      console.log(`${TAG} → Request body:`, BODY);

      const res = await fetch(URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(BODY),
      });

      console.log(`${TAG} → HTTP status:`, res.status, res.statusText);

      let json;
      try {
        json = await res.json();
      } catch (parseErr) {
        console.error(`${TAG} ❌ JSON parse error:`, parseErr);
        throw parseErr;
      }

      console.log(`${TAG} → Raw JSON:`, JSON.stringify(json, null, 2));

      // Defensive extraction: accept several shapes
      const rows =
        Array.isArray(json?.RESULT) ? json.RESULT :
        Array.isArray(json?.rows) ? json.rows :
        Array.isArray(json?.data) ? json.data :
        [];

      console.log(`${TAG} → Derived rows count:`, rows.length);

      SET_RESULT_SET_P_CLIENT_SONG_SCORE_GET(rows);
    } catch (err) {
      console.error(`${TAG} ❌ REFRESH failed:`, err);
    }
  };

  // Expose REFRESH to parent
  useImperativeHandle(ref, () => {
    console.log(`${TAG} → useImperativeHandle wired (REFRESH exposed)`);
    return { REFRESH };
  }, []);

  // ---- DERIVED RENDER LISTS -------------------------------------------------
  const SCORE_SUMMARY = RESULT_SET_P_CLIENT_SONG_SCORE_GET.filter(
    i => i?.SCORE_SUMMARY_OR_DETAIL === 'SUMMARY'
  );
  const SCORE_DETAIL = RESULT_SET_P_CLIENT_SONG_SCORE_GET.filter(
    i => i?.SCORE_SUMMARY_OR_DETAIL === 'DETAIL'
  );

  useEffect(() => {
    console.log(`${TAG} → RENDER state:
      RESULT_SET length=${RESULT_SET_P_CLIENT_SONG_SCORE_GET.length},
      SUMMARY=${SCORE_SUMMARY.length},
      DETAIL=${SCORE_DETAIL.length}
    `);
  }, [RESULT_SET_P_CLIENT_SONG_SCORE_GET.length]);

  // ---- TAP → notify parent only (no local refresh) --------------------------
  const USER_TAPPED_SUMMARY_ROW = (breakdownName) => {
    console.log(`${TAG} → USER_TAPPED_SUMMARY_ROW breakdownName=`, breakdownName);
    USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED?.(breakdownName);
  };

  // ---- RENDER ---------------------------------------------------------------
  return (
    <View style={styles.container}>
      <ScrollView contentContainerStyle={styles.scrollContainer} horizontal={false}>
        <View style={styles.columnWrapper}>
          <View style={styles.leftColumn}>
            {SCORE_SUMMARY.length === 0 && (
              <Text style={styles.debugText}>[Scores] No SUMMARY rows to display</Text>
            )}
            {SCORE_SUMMARY.map((item, idx) => (
              <TouchableOpacity
                key={`summary-${idx}`}
                onPress={() => USER_TAPPED_SUMMARY_ROW(item?.BREAKDOWN_NAME)}
              >
                <Text style={[styles.scoreText, { marginLeft: (item?.DISPLAY_INDENT_LEVEL_NO || 0) * 8 }]}>
                  {item?.DISPLAY_TEXT ?? '(no DISPLAY_TEXT)'}
                </Text>
              </TouchableOpacity>
            ))}
          </View>

          <View style={styles.rightColumn}>
            {SCORE_DETAIL.length === 0 && (
              <Text style={styles.debugText}>[Scores] No DETAIL rows to display</Text>
            )}
            {SCORE_DETAIL.map((item, idx) => (
              <Text
                key={`detail-${idx}`}
                style={[styles.scoreText, { marginLeft: (item?.DISPLAY_INDENT_LEVEL_NO || 0) * 8 }]}
              >
                {item?.DISPLAY_TEXT ?? '(no DISPLAY_TEXT)'}
              </Text>
            ))}
          </View>
        </View>
      </ScrollView>
    </View>
  );
});

export default SCREEN_MAIN_5_SCORES;

const styles = StyleSheet.create({
  container: { flex: 1 },
  scrollContainer: { flexDirection: 'row', justifyContent: 'center', paddingHorizontal: 10 },
  columnWrapper: { flexDirection: 'row', width: '100%' },
  leftColumn: { flex: 1, paddingRight: 10, borderRightWidth: 1, borderColor: '#ddd' },
  rightColumn: { flex: 1, paddingLeft: 10 },
  scoreText: { fontSize: 14, paddingVertical: 4, color: '#333' },
  debugText: { fontSize: 12, color: '#999', paddingVertical: 4 },
});
