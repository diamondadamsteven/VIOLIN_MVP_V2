// SCREEN_MAIN_2b_CONDUCTOR.js
import { useEffect, useState } from 'react';
import { Image, StyleSheet, Text, View } from 'react-native';
import { batonImages, conductorFaces, thoughtBubble } from './assets/IMAGES';

export default function SCREEN_MAIN_2b_CONDUCTOR() {
  const [beatIndex, setBeatIndex] = useState(0);
  const [mood, setMood] = useState('neutral');
  const [instruction, setInstruction] = useState("I'll give you 2 bars for nothing");

  useEffect(() => {
    const interval = setInterval(() => {
      setBeatIndex((prev) => (prev + 1) % 4);
    }, 600);
    return () => clearInterval(interval);
  }, []);

  return (
    <View style={styles.container}>
      {/* Left Baton */}
      <Image style={styles.baton} source={batonImages.left[beatIndex]} />

      {/* Face with attached thought bubble */}
      <View style={styles.faceContainer}>
        {/* Conductor Face */}
        <Image style={styles.face} source={conductorFaces[mood]} />

        {/* Thought Bubble to the right */}
        <View style={styles.bubbleWrapper}>
          <Image source={thoughtBubble} style={styles.thoughtBubble} />
          <Text style={styles.instructionText}>{instruction}</Text>
        </View>
      </View>

      {/* Right Baton */}
      <Image style={styles.baton} source={batonImages.right[beatIndex]} />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'flex-end',
    justifyContent: 'center',
    marginTop: 8,
    paddingHorizontal: 8,
  },
  baton: {
    width: 50,
    height: 70,
    resizeMode: 'contain',
    marginHorizontal: 4,
  },
  faceContainer: {
    position: 'relative',
    alignItems: 'center',
    justifyContent: 'center',
    marginHorizontal: 4,
  },
  face: {
    width: 80,
    height: 80,
    resizeMode: 'contain',
  },
  bubbleWrapper: {
    position: 'absolute',
    right: -120,
    top: -10,
    width: 140,
    height: 80,
    alignItems: 'center',
    justifyContent: 'center',
  },
  thoughtBubble: {
    width: '100%',
    height: '100%',
    resizeMode: 'contain',
  },
  instructionText: {
    position: 'absolute',
    top: 18,
    left: 10,
    right: 10,
    textAlign: 'center',
    fontSize: 11,
    color: '#333',
  },
});
