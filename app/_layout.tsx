import { Slot } from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { useEffect } from 'react';
import { CLIENT_STEP_1_REGISTER } from '../CLIENT_STEP_1_REGISTER.js';

export default function Layout() {
  useEffect(() => {
    (async () => {
      await CLIENT_STEP_1_REGISTER();
    })();
  }, []);

  return (
    <>
      <Slot />
      <StatusBar style="auto" />
    </>
  );
}
