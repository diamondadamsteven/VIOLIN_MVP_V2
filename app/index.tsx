// app/index.tsx
import { Link } from 'expo-router';
import React from 'react';
import { StyleSheet, View } from 'react-native';
import SCREEN_LANDING_PAGE from '../SCREEN_LANDING_PAGE';

export default function IndexScreen() {
  return (
    <View style={styles.container}>
      <SCREEN_LANDING_PAGE />

      {/* ── TEMP: Debug entry to WS Diagnostic ───────────────────────────── */}
      <Link href="/debug/ws-diagnostic" style={styles.debugLink}>
        Open WS Diagnostic
      </Link>
      {/* Remove the block above once you’re done debugging */}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  debugLink: {
    margin: 12,
    padding: 12,
    borderRadius: 10,
    backgroundColor: '#eee',
    textAlign: 'center',
    fontWeight: '600',
  },
});
