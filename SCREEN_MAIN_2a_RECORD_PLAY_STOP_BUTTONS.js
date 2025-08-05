// SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS.js
import { useEffect, useState } from 'react';
import { Button, StyleSheet, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_MAIN_2a_RECORD_PLAY_STOP_BUTTONS({ onPlay, onRecord, onPause, onStop }) {
  const [isPlaying, setIsPlaying] = useState(false);
  const [isRecording, setIsRecording] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const hasRecording = !!CLIENT_APP_VARIABLES.RECORDING_ID;

  useEffect(() => {
    // Reset all state if recording ID changes
    setIsPlaying(false);
    setIsRecording(false);
    setIsPaused(false);
  }, [CLIENT_APP_VARIABLES.RECORDING_ID]);

  const handlePlay = () => {
    setIsPlaying(true);
    setIsRecording(false);
    setIsPaused(false);
    onPlay && onPlay();
  };

  const handleRecord = () => {
    setIsRecording(true);
    setIsPlaying(false);
    setIsPaused(false);
    onRecord && onRecord();
  };

  const handlePause = () => {
    setIsPaused(true);
    onPause && onPause();
  };

  const handleStop = () => {
    setIsPaused(false);
    setIsPlaying(false);
    setIsRecording(false);
    onStop && onStop();
  };

  return (
    <View style={styles.container}>
      {!isPlaying && !isRecording && !isPaused && (
        <>
          {!hasRecording && <Button title="Record" onPress={handleRecord} />}
          <Button title="Play" onPress={handlePlay} />
        </>
      )}

      {(isPlaying || isRecording) && !isPaused && (
        <>
          <Button title="Pause" onPress={handlePause} />
          <Button title="Stop" onPress={handleStop} />
        </>
      )}

      {isPaused && (
        <>
          {!hasRecording && <Button title="Record" onPress={handleRecord} />}
          <Button title="Play" onPress={handlePlay} />
        </>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    gap: 10,
    marginVertical: 8,
    justifyContent: 'center',
  },
});
