import { useEffect, useRef } from 'react';
import { ScrollView, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

import SCREEN_MAIN_1_RECORDING_PARAMETERS from './SCREEN_MAIN_1_RECORDING_PARAMETERS';
import SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS from './SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS';
import SCREEN_MAIN_2b_CONDUCTOR from './SCREEN_MAIN_2b_CONDUCTOR';
import SCREEN_MAIN_3_MUSIC_NOTES from './SCREEN_MAIN_3_MUSIC_NOTES';
import SCREEN_MAIN_4_COLOR_CHART from './SCREEN_MAIN_4_COLOR_CHART';
import SCREEN_MAIN_5_SCORES from './SCREEN_MAIN_5_SCORES';
import SCREEN_MAIN_6_COMMAND_BUTTONS from './SCREEN_MAIN_6_COMMAND_BUTTONS';

export default function SCREEN_MAIN() {
  // ── REFS (ALL CAPS, descriptive) ───────────────────────────
  const REF_SCREEN_MAIN_2b_CONDUCTOR = useRef(null);
  const REF_SCREEN_MAIN_3_MUSIC_NOTES = useRef(null);
  const REF_SCREEN_MAIN_4_COLOR_CHART = useRef(null);
  const REF_SCREEN_MAIN_5_SCORES = useRef(null);
  const REF_SCREEN_MAIN_6_COMMAND_BUTTONS = useRef(null);

  // ── USER EVENTS (main screen only) ─────────────────────────
  const USER_EVENT_PLAY_BUTTON_TAPPED = () => {
    // Conductor control
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

  // Orchestrate multi-panel refresh on score benchmark tap
  const USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED = async (breakdownName) => {
    if (!breakdownName) return;

    CLIENT_APP_VARIABLES.BREAKDOWN_NAME = breakdownName;

    REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH();
    REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH();
    REF_SCREEN_MAIN_5_SCORES.current?.REFRESH();
  };

  // ── INITIAL LOAD ────────────────────────────────────────────
  useEffect(() => {
    if (!CLIENT_APP_VARIABLES.BREAKDOWN_NAME) {
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    }

    // Initialize conductor
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.STOP_BATONS();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE('neutral');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT("I'll give you 2 bars for nothing");

    //if (CLIENT_APP_VARIABLES.RECORDING_ID) {
    // One-time fetch on mount
    REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH();
    REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH();
    REF_SCREEN_MAIN_5_SCORES.current?.REFRESH();
    REF_SCREEN_MAIN_6_COMMAND_BUTTONS.current?.REFRESH();
    //}
  }, []);

  return (
    <ScrollView>
      <SCREEN_MAIN_1_RECORDING_PARAMETERS />

      <View
        style={{
          flexDirection: 'row',
          alignItems: 'center',
          justifyContent: 'flex-start',
          paddingHorizontal: 12,
          marginTop: 0,
          paddingTop: 0,
          marginBottom: 4,
        }}
      >
        <SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS
          USER_EVENT_PLAY_BUTTON_TAPPED={USER_EVENT_PLAY_BUTTON_TAPPED}
          USER_EVENT_RECORD_BUTTON_TAPPED={USER_EVENT_RECORD_BUTTON_TAPPED}
          USER_EVENT_PAUSE_BUTTON_TAPPED={USER_EVENT_PAUSE_BUTTON_TAPPED}
          USER_EVENT_STOP_BUTTON_TAPPED={USER_EVENT_STOP_BUTTON_TAPPED}
        />
        <SCREEN_MAIN_2b_CONDUCTOR ref={REF_SCREEN_MAIN_2b_CONDUCTOR} />
      </View>

      <SCREEN_MAIN_3_MUSIC_NOTES ref={REF_SCREEN_MAIN_3_MUSIC_NOTES} />
      <SCREEN_MAIN_4_COLOR_CHART ref={REF_SCREEN_MAIN_4_COLOR_CHART} />
      <SCREEN_MAIN_5_SCORES 
        ref={REF_SCREEN_MAIN_5_SCORES}
        USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED={USER_EVENT_SCORE_BENCHMARK_NAME_TAPPED}
      />
      <SCREEN_MAIN_6_COMMAND_BUTTONS ref={REF_SCREEN_MAIN_6_COMMAND_BUTTONS} />
    </ScrollView>
  );
}
