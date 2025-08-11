import { useEffect, useRef, useState } from 'react';
import { DeviceEventEmitter, ScrollView, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

import SCREEN_MAIN_1_RECORDING_PARAMETERS from './SCREEN_MAIN_1_RECORDING_PARAMETERS';
import SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS, {
  EVT_CONDUCTOR_UPDATED,
  EVT_NOTES_UPDATED,
  EVT_SCORES_UPDATED,
} from './SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS';
import SCREEN_MAIN_2b_CONDUCTOR from './SCREEN_MAIN_2b_CONDUCTOR';
import SCREEN_MAIN_3_MUSIC_NOTES from './SCREEN_MAIN_3_MUSIC_NOTES';
import SCREEN_MAIN_4_COLOR_CHART from './SCREEN_MAIN_4_COLOR_CHART';
import SCREEN_MAIN_5_SCORES from './SCREEN_MAIN_5_SCORES';
import SCREEN_MAIN_6_COMMAND_BUTTONS from './SCREEN_MAIN_6_COMMAND_BUTTONS';

export default function SCREEN_MAIN() {
  const REF_SCREEN_MAIN_2b_CONDUCTOR = useRef(null);
  const REF_SCREEN_MAIN_3_MUSIC_NOTES = useRef(null);
  const REF_SCREEN_MAIN_4_COLOR_CHART = useRef(null);
  const REF_SCREEN_MAIN_5_SCORES = useRef(null);
  const REF_SCREEN_MAIN_6_COMMAND_BUTTONS = useRef(null);

  const [leftWidth, setLeftWidth] = useState(0);

  // ----- Live event listeners (notes/scores/conductor) -----
  useEffect(() => {
    // Notes changed → refresh notation, color chart, and (optionally) scores
    const subNotes = DeviceEventEmitter.addListener(EVT_NOTES_UPDATED, () => {
      REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH?.();
      REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH?.();
      REF_SCREEN_MAIN_5_SCORES.current?.REFRESH?.();
    });

    // Scores changed → refresh scores panel
    const subScores = DeviceEventEmitter.addListener(EVT_SCORES_UPDATED, () => {
      REF_SCREEN_MAIN_5_SCORES.current?.REFRESH?.();
    });

    // Conductor update → set face/thought and optionally baton motion
    const subCond = DeviceEventEmitter.addListener(EVT_CONDUCTOR_UPDATED, ({ animation }) => {
      const a = animation || {};
      const mood = String(a.CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL || 'neutral').toLowerCase();
      const text = a.CONDUCTOR_MESSAGE_TEXT ?? '';

      REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE?.(mood);
      REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT?.(text);

      if (a.YN_START_BATONS === 'Y') REF_SCREEN_MAIN_2b_CONDUCTOR.current?.START_BATONS?.();
      if (a.YN_STOP_BATONS === 'Y') REF_SCREEN_MAIN_2b_CONDUCTOR.current?.STOP_BATONS?.();
    });

    return () => {
      subNotes.remove();
      subScores.remove();
      subCond.remove();
    };
  }, []);

  // ----- Button callbacks -----
  const USER_EVENT_PLAY_BUTTON_TAPPED = () => {
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.START_BATONS();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE('happy');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT("Let's make music!");
    console.log('Play button pressed');
  };

  const USER_EVENT_RECORD_BUTTON_TAPPED = () => {
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.START_BATONS();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE('neutral');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT('Recording…');
    console.log('Record button pressed');
  };

  const USER_EVENT_PAUSE_BUTTON_TAPPED = () => {
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.STOP_BATONS();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE('neutral');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT('Paused');
    console.log('Pause button pressed');
  };

  const USER_EVENT_STOP_BUTTON_TAPPED = () => {
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.STOP_BATONS();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE('neutral');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT('');
    console.log('Stop button pressed');
  };

  const USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED = async (breakdownName) => {
    if (!breakdownName) return;
    CLIENT_APP_VARIABLES.BREAKDOWN_NAME = breakdownName;
    REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH?.();
    REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH?.();
    REF_SCREEN_MAIN_5_SCORES.current?.REFRESH?.();
  };

  // ----- Initial load -----
  useEffect(() => {
    if (!CLIENT_APP_VARIABLES.BREAKDOWN_NAME) {
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    }
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.STOP_BATONS?.();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE?.('neutral');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT?.("I'll give you 2 bars for nothing");

    REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH?.();
    REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH?.();
    REF_SCREEN_MAIN_5_SCORES.current?.REFRESH?.();
    REF_SCREEN_MAIN_6_COMMAND_BUTTONS.current?.REFRESH?.();
  }, []);

  return (
    <ScrollView>
      {/* compact top section */}
      <View style={{ paddingHorizontal: 12, paddingTop: 4, paddingBottom: 2 }}>
        <SCREEN_MAIN_1_RECORDING_PARAMETERS density="compact" />
      </View>

      {/* Transport (left) + Conductor (center) + Right spacer (mirror left) */}
      <View
        style={{
          flexDirection: 'row',
          alignItems: 'center',
          paddingHorizontal: 12,
          marginBottom: 4,
        }}
      >
        {/* Left controls (measure to mirror-right for perfect center) */}
        <View
          onLayout={(e) => setLeftWidth(e.nativeEvent.layout.width)}
          collapsable={false}
        >
          <SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS
            USER_EVENT_PLAY_BUTTON_TAPPED={USER_EVENT_PLAY_BUTTON_TAPPED}
            USER_EVENT_RECORD_BUTTON_TAPPED={USER_EVENT_RECORD_BUTTON_TAPPED}
            USER_EVENT_PAUSE_BUTTON_TAPPED={USER_EVENT_PAUSE_BUTTON_TAPPED}
            USER_EVENT_STOP_BUTTON_TAPPED={USER_EVENT_STOP_BUTTON_TAPPED}
          />
        </View>

        {/* Center conductor */}
        <View style={{ flex: 1, alignItems: 'center' }}>
          <SCREEN_MAIN_2b_CONDUCTOR ref={REF_SCREEN_MAIN_2b_CONDUCTOR} />
        </View>

        {/* Right spacer mirrors left width to keep true center */}
        <View style={{ width: leftWidth }} />
      </View>

      <SCREEN_MAIN_3_MUSIC_NOTES ref={REF_SCREEN_MAIN_3_MUSIC_NOTES} />
      <SCREEN_MAIN_4_COLOR_CHART ref={REF_SCREEN_MAIN_4_COLOR_CHART} />
      <SCREEN_MAIN_5_SCORES
        ref={REF_SCREEN_MAIN_5_SCORES}
        USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED={USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED}
      />

      <View style={{ alignItems: 'center', marginTop: 8 }}>
        <SCREEN_MAIN_6_COMMAND_BUTTONS ref={REF_SCREEN_MAIN_6_COMMAND_BUTTONS} />
      </View>
    </ScrollView>
  );
}
