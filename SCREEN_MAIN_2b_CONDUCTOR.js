// SCREEN_MAIN_2b_CONDUCTOR.js
import { forwardRef, useImperativeHandle, useRef, useState } from 'react';
import { Image, StyleSheet, Text, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';
import { batonImages, conductorFaces } from './assets/IMAGES';

const SCREEN_MAIN_2b_CONDUCTOR = forwardRef((props, ref) => {
  const [BEAT_INDEX, SET_BEAT_INDEX] = useState(0);
  const [FACE_EXPRESSION, SET_FACE_EXPRESSION] = useState('neutral');
  const [THOUGHT_TEXT, SET_THOUGHT_TEXT] = useState("I'll give you 2 bars for nothing");

  const BATON_INTERVAL_REF = useRef(null);

  // Public methods for SCREEN_MAIN
  useImperativeHandle(ref, () => ({
    START_BATONS: () => {
      STOP_BATONS_INTERNAL(); // Ensure no double interval
      const bpm = CLIENT_APP_VARIABLES.BPM || 60;
      const msPerBeat = 60000 / bpm;
      BATON_INTERVAL_REF.current = setInterval(() => {
        SET_BEAT_INDEX((prev) => (prev + 1) % 4);
      }, msPerBeat);
    },
    STOP_BATONS: () => {
      STOP_BATONS_INTERNAL();
      SET_BEAT_INDEX(0);
    },
    SET_FACE: (expression) => {
      SET_FACE_EXPRESSION(expression);
    },
    SET_THOUGHT: (text) => {
      SET_THOUGHT_TEXT(text);
    },
  }));

  const STOP_BATONS_INTERNAL = () => {
    if (BATON_INTERVAL_REF.current) {
      clearInterval(BATON_INTERVAL_REF.current);
      BATON_INTERVAL_REF.current = null;
    }
  };

  return (
    <View style={styles.container}>
      <Image style={styles.baton} source={batonImages.left[BEAT_INDEX]} />

      <View style={styles.faceContainer}>
        <Image style={styles.face} source={conductorFaces[FACE_EXPRESSION]} />
      </View>

      <Image style={styles.baton} source={batonImages.right[BEAT_INDEX]} />

      <View style={styles.bubbleContainer}>
        <View style={styles.bubbleTail} />
        <View style={styles.bubbleBox}>
          <Text style={styles.instructionText}>{THOUGHT_TEXT}</Text>
        </View>
      </View>
    </View>
  );
});

export default SCREEN_MAIN_2b_CONDUCTOR;

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'flex-end',
    justifyContent: 'center',
    paddingHorizontal: 8,
    marginTop: 0,
    marginBottom: 0,
  },
  baton: {
    width: 36,
    height: 64,
    resizeMode: 'contain',
    marginHorizontal: 2,
  },
  faceContainer: {
    alignItems: 'center',
    justifyContent: 'center',
    marginHorizontal: 4,
  },
  face: {
    width: 80,
    height: 80,
    resizeMode: 'contain',
  },
  bubbleContainer: {
    position: 'absolute',
    right: 10,
    bottom: 55,
    alignItems: 'flex-end',
  },
  bubbleTail: {
    width: 0,
    height: 0,
    borderLeftWidth: 12,
    borderLeftColor: 'transparent',
    borderRightWidth: 12,
    borderRightColor: 'transparent',
    borderBottomWidth: 20,
    borderBottomColor: '#fff',
    transform: [{ rotate: '-45deg' }],
    marginRight: 30,
    marginBottom: -8,
  },
  bubbleBox: {
    maxWidth: 180,
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 8,
    shadowColor: '#000',
    shadowOffset: { width: 1, height: 1 },
    shadowOpacity: 0.3,
    shadowRadius: 2,
    elevation: 2,
  },
  instructionText: {
    fontSize: 11,
    color: '#333',
    textAlign: 'center',
  },
});
