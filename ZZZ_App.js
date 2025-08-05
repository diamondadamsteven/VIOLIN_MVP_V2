// App.js
// app/_layout.tsx
import { Slot } from 'expo-router';
import { useEffect, useState } from 'react';
import { ActivityIndicator, StyleSheet, View } from 'react-native';

import { CLIENT_STEP_1_REGISTER } from '../CLIENT_STEP_1_REGISTER';

export default function Layout() {
  const [isReady, setIsReady] = useState(false);

  useEffect(() => {
    async function APP_INIT() {
      await CLIENT_STEP_1_REGISTER();
      setIsReady(true);
    }
    APP_INIT();
  }, []);

  if (!isReady) {
    return (
      <View style={styles.container}>
        <ActivityIndicator size="large" />
      </View>
    );
  }

  return <Slot />;
}

const styles = StyleSheet.create({
  container: { flex: 1, justifyContent: 'center', alignItems: 'center' },
});
