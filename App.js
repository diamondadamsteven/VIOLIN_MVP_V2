// App.js

import { useEffect, useState } from 'react';
import { ActivityIndicator, StyleSheet, Text, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';
import CLIENT_STEP1_REGISTRATION_RUN from './CLIENT_STEP_1_REGISTRATION';

export default function APP() {
  const [IS_READY, SET_IS_READY] = useState(false);

  useEffect(() => {
    async function APP_INIT() {
      await CLIENT_STEP1_REGISTRATION_RUN();
      SET_IS_READY(true);
    }

    APP_INIT();
  }, []);

  if (!IS_READY) {
    return (
      <View style={STYLES.CONTAINER}>
        <ActivityIndicator size="large" />
        <Text>Initializing App...</Text>
      </View>
    );
  }

  const VIOLINIST_ID = CLIENT_APP_VARIABLES.VIOLINIST_ID;

  return (
    <View style={STYLES.CONTAINER}>
      <Text>Welcome, Violinist ID:</Text>
      <Text style={STYLES.ID}>{VIOLINIST_ID || 'Unknown'}</Text>
    </View>
  );
}

const STYLES = StyleSheet.create({
  CONTAINER: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  ID: {
    marginTop: 10,
    fontWeight: 'bold',
    fontSize: 18,
  },
});
