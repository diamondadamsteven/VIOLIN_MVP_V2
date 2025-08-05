// SCREEN_MAIN.js
import { ScrollView, View } from 'react-native';

import SCREEN_MAIN_1_RECORDING_PARAMETERS from './SCREEN_MAIN_1_RECORDING_PARAMETERS';
import SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS from './SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS';
import SCREEN_MAIN_2b_CONDUCTOR from './SCREEN_MAIN_2b_CONDUCTOR';
import SCREEN_MAIN_3_MUSIC_NOTES from './SCREEN_MAIN_3_MUSIC_NOTES';
// import ColorChart from './SCREEN_MAIN_4_COLOR_CHART';
// import Scores from './SCREEN_MAIN_5_SCORES';
// import CommandButtons from './SCREEN_MAIN_6_COMMAND_BUTTONS';

export default function SCREEN_MAIN() {
  // Dummy placeholders – replace with real logic later
  const handlePlay = () => console.log('Play button pressed');
  const handleRecord = () => console.log('Record button pressed');
  const handlePause = () => console.log('Pause button pressed');
  const handleStop = () => console.log('Stop button pressed');

  return (
    <ScrollView>
      <SCREEN_MAIN_1_RECORDING_PARAMETERS />

      <View style={{ flexDirection: 'row', alignItems: 'center', justifyContent: 'center', gap: 12 }}>
        <SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS
          onPlay={handlePlay}
          onRecord={handleRecord}
          onPause={handlePause}
          onStop={handleStop}
        />
        <SCREEN_MAIN_2b_CONDUCTOR />
      </View>

      <SCREEN_MAIN_3_MUSIC_NOTES />
      {/* <ColorChart />
      <Scores />
      <CommandButtons /> */}
    </ScrollView>
  );
}
