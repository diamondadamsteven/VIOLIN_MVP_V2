// app/_layout.tsx
import { useFonts } from 'expo-font';
import { Slot } from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { useEffect, useState } from 'react';
import { ActivityIndicator, StyleSheet, View } from 'react-native';
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';

import APP_NAVIGATION_BAR from '../APP_NAVIGATION_BAR';
import { CLIENT_STEP_1_REGISTER } from '../CLIENT_STEP_1_REGISTER';

export default function Layout() {
  const [isReady, setIsReady] = useState(false);

  // Load Noto Music once for the whole app
  const [fontsLoaded] = useFonts({
    NotoMusic: require('../assets/FONTS_NotoMusic-Regular.ttf'), // <— your custom path/name
  });

  useEffect(() => {
    async function APP_INIT() {
      await CLIENT_STEP_1_REGISTER();
      setIsReady(true);
    }
    APP_INIT();
  }, []);

  if (!isReady || !fontsLoaded) {
    return (
      <View style={styles.loading}>
        <ActivityIndicator size="large" />
      </View>
    );
  }

  return (
    <SafeAreaProvider>
      <SafeAreaView style={styles.safeArea} edges={['top', 'left', 'right', 'bottom']}>
        <StatusBar style="dark" />
        <View style={styles.topSpacer} />
        <View style={styles.appColumn}>
          <View style={styles.content}>
            <Slot />
          </View>
          <View style={styles.navWrapper}>
            <APP_NAVIGATION_BAR />
          </View>
        </View>
      </SafeAreaView>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  safeArea: { flex: 1, backgroundColor: '#fff' },
  appColumn: { flex: 1 },
  content: { flex: 1 },
  navWrapper: { borderTopWidth: StyleSheet.hairlineWidth, borderTopColor: '#e5e5e5' },
  topSpacer: { height: 12 },
  loading: { flex: 1, justifyContent: 'center', alignItems: 'center', backgroundColor: '#fff' },
});
