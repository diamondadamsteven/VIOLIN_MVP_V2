import { FontAwesome } from '@expo/vector-icons';
import { useEffect, useState } from 'react';
import { StyleSheet, TouchableOpacity, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS({
  USER_EVENT_PLAY_BUTTON_TAPPED,
  USER_EVENT_RECORD_BUTTON_TAPPED,
  USER_EVENT_PAUSE_BUTTON_TAPPED,
  USER_EVENT_STOP_BUTTON_TAPPED,
}) {
  const [IS_PLAYING, SET_IS_PLAYING] = useState(false);
  const [IS_RECORDING, SET_IS_RECORDING] = useState(false);
  const [IS_PAUSED, SET_IS_PAUSED] = useState(false);
  const HAS_RECORDING = !!CLIENT_APP_VARIABLES.RECORDING_ID;

  useEffect(() => {
    SET_IS_PLAYING(false);
    SET_IS_RECORDING(false);
    SET_IS_PAUSED(false);
  }, [CLIENT_APP_VARIABLES.RECORDING_ID]);

  const PLAY_TAPPED = () => {
    SET_IS_PLAYING(true);
    SET_IS_RECORDING(false);
    SET_IS_PAUSED(false);
    USER_EVENT_PLAY_BUTTON_TAPPED?.();
  };

  const RECORD_TAPPED = () => {
    SET_IS_RECORDING(true);
    SET_IS_PLAYING(false);
    SET_IS_PAUSED(false);
    USER_EVENT_RECORD_BUTTON_TAPPED?.();
  };

  const PAUSE_TAPPED = () => {
    SET_IS_PAUSED(true);
    USER_EVENT_PAUSE_BUTTON_TAPPED?.();
  };

  const STOP_TAPPED = () => {
    SET_IS_PAUSED(false);
    SET_IS_PLAYING(false);
    SET_IS_RECORDING(false);
    USER_EVENT_STOP_BUTTON_TAPPED?.();
  };

  return (
    <View style={STYLES.container}>
      {!IS_PLAYING && !IS_RECORDING && !IS_PAUSED && (
        <>
          {!HAS_RECORDING && (
            <TouchableOpacity onPress={RECORD_TAPPED} style={STYLES.circleButton}>
              <FontAwesome name="circle" size={32} color="red" />
            </TouchableOpacity>
          )}
          <TouchableOpacity onPress={PLAY_TAPPED} style={STYLES.circleButton}>
            <FontAwesome name="play-circle" size={32} color="black" />
          </TouchableOpacity>
        </>
      )}

      {(IS_PLAYING || IS_RECORDING) && !IS_PAUSED && (
        <>
          <TouchableOpacity onPress={PAUSE_TAPPED} style={STYLES.circleButton}>
            <FontAwesome name="pause-circle" size={32} color="black" />
          </TouchableOpacity>
          <TouchableOpacity onPress={STOP_TAPPED} style={STYLES.circleButton}>
            <FontAwesome name="stop-circle" size={32} color="black" />
          </TouchableOpacity>
        </>
      )}

      {IS_PAUSED && (
        <>
          {!HAS_RECORDING && (
            <TouchableOpacity onPress={RECORD_TAPPED} style={STYLES.circleButton}>
              <FontAwesome name="circle" size={32} color="red" />
            </TouchableOpacity>
          )}
          <TouchableOpacity onPress={PLAY_TAPPED} style={STYLES.circleButton}>
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
    marginVertical: 4, // ↓ reduced vertical margin
    justifyContent: 'flex-start',
    alignItems: 'flex-start',
  },
  circleButton: {
    padding: 4,
  },
});
