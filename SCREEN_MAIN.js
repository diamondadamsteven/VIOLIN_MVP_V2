import { useEffect, useRef, useState } from 'react';
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
  const REF_SCREEN_MAIN_2b_CONDUCTOR = useRef(null);
  const REF_SCREEN_MAIN_3_MUSIC_NOTES = useRef(null);
  const REF_SCREEN_MAIN_4_COLOR_CHART = useRef(null);
  const REF_SCREEN_MAIN_5_SCORES = useRef(null);
  const REF_SCREEN_MAIN_6_COMMAND_BUTTONS = useRef(null);

  const [leftWidth, setLeftWidth] = useState(0);

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
    REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH();
    REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH();
    REF_SCREEN_MAIN_5_SCORES.current?.REFRESH();
  };

  useEffect(() => {
    if (!CLIENT_APP_VARIABLES.BREAKDOWN_NAME) {
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    }
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.STOP_BATONS();
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_FACE('neutral');
    REF_SCREEN_MAIN_2b_CONDUCTOR.current?.SET_THOUGHT("I'll give you 2 bars for nothing");

    REF_SCREEN_MAIN_3_MUSIC_NOTES.current?.REFRESH();
    REF_SCREEN_MAIN_4_COLOR_CHART.current?.REFRESH();
    REF_SCREEN_MAIN_5_SCORES.current?.REFRESH();
    REF_SCREEN_MAIN_6_COMMAND_BUTTONS.current?.REFRESH();
  }, []);

  return (
    <ScrollView>
      {/* ↓↓↓ compact = ~33% less vertical space */}
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

        <View style={{ flex: 1, alignItems: 'center' }}>
          <SCREEN_MAIN_2b_CONDUCTOR ref={REF_SCREEN_MAIN_2b_CONDUCTOR} />
        </View>

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
