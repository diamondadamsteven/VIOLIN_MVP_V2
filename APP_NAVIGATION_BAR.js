// APP_NAVIGATION_BAR.js
import { router } from 'expo-router';
import React, { useRef } from 'react';
import { SafeAreaView, StyleSheet, Text, TouchableOpacity, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function APP_NAVIGATION_BAR() {
  // simple guard to prevent rapid double navigations
  const busyRef = useRef(false);
  const go = (fn) => {
    if (busyRef.current) return;
    busyRef.current = true;
    requestAnimationFrame(() => {
      fn();
      setTimeout(() => (busyRef.current = false), 350);
    });
  };

  const goHome = () => go(() => router.replace('/')); // landing page

  const goCompose = () =>
    go(() => {
      CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE = 'COMPOSE';
      // optional: clear song/recording so compose starts clean
      CLIENT_APP_VARIABLES.SONG_ID = null;
      CLIENT_APP_VARIABLES.RECORDING_ID = null;
      router.replace('/SCREEN_MAIN');
    });

  const goPlay = () =>
    go(() => {
      CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE = 'PLAY';
      router.replace('/SCREEN_SONG_SEARCH'); // pick song, then into SCREEN_MAIN
    });

  const goNetwork = () => go(() => router.replace('/SCREEN_NETWORKING')); // stub for now

  return (
    <SafeAreaView edges={['bottom']} style={styles.safe}>
      <View style={styles.bar}>
        <NavItem label="Home" icon="🏠" onPress={goHome} />
        <NavItem label="Compose" icon="🎵➕" onPress={goCompose} />
        <NavItem label="Play" icon="❓" onPress={goPlay} />
        <NavItem label="Network" icon="👤" onPress={goNetwork} />
      </View>
    </SafeAreaView>
  );
}

function NavItem({ icon, label, onPress }) {
  return (
    <TouchableOpacity
      onPress={onPress}
      accessibilityRole="button"
      accessibilityLabel={label}
      hitSlop={{ top: 10, bottom: 10, left: 12, right: 12 }}
      style={styles.item}
      activeOpacity={0.7}
    >
      <Text style={styles.icon}>{icon}</Text>
    </TouchableOpacity>
  );
}

const styles = StyleSheet.create({
  safe: {
    backgroundColor: 'transparent',
  },
  bar: {
    height: 64,
    backgroundColor: '#111',
    borderTopLeftRadius: 28,
    borderTopRightRadius: 28,
    flexDirection: 'row',
    justifyContent: 'space-around',
    alignItems: 'center',
    paddingHorizontal: 12,
    paddingBottom: 6, // room for the iOS home indicator curve
  },
  item: { padding: 8 },
  icon: { fontSize: 20, color: '#fff' },
});
