// CLIENT_STEP_1_REGISTER.js
import * as Device from 'expo-device';
import * as Location from 'expo-location';
import * as SecureStore from 'expo-secure-store';
import { DEBUG_CONSOLE_LOG } from './CLIENT_APP_FUNCTIONS';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export async function CLIENT_STEP_1_REGISTER () {
  try {
    // 1. Gather Device ID
    CLIENT_APP_VARIABLES.DEVICE_ID = Device.osInternalBuildId || Device.deviceName || 'unknown_device';

    // 2. Get IP Address
    try {
      const ipRes = await fetch('https://api.ipify.org?format=json');
      const ipData = await ipRes.json();
      CLIENT_APP_VARIABLES.IP_ADDRESS = ipData.ip;
    } catch {};

    // 3. Get Geolocation
    try {
      const { status } = await Location.requestForegroundPermissionsAsync();
      if (status === 'granted') {
        const location = await Location.getCurrentPositionAsync({});
        CLIENT_APP_VARIABLES.LATITUDE = location.coords.latitude;
        CLIENT_APP_VARIABLES.LONGITUDE = location.coords.longitude;
      }
    } catch {}

    // 4. Register Violinist and/or Get Violinist_ID
    const P_CLIENT_VIOLINIST_INS = {
      SP_NAME: "P_CLIENT_VIOLINIST_INS",
      PARAMS: {
        DEVICE_ID: CLIENT_APP_VARIABLES.DEVICE_ID,
        IP_ADDRESS: CLIENT_APP_VARIABLES.IP_ADDRESS,  
        LATITUDE: CLIENT_APP_VARIABLES.LATITUDE,
        LONGITUDE: CLIENT_APP_VARIABLES.LONGITUDE
      }
    };

    const P_CLIENT_VIOLINIST_INS_response = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(P_CLIENT_VIOLINIST_INS)
    });

    const P_CLIENT_VIOLINIST_INS_data = await P_CLIENT_VIOLINIST_INS_response.json();
    console.log('P_CLIENT_VIOLINIST_INS_data:', JSON.stringify(P_CLIENT_VIOLINIST_INS_data, null, 2));

    CLIENT_APP_VARIABLES.VIOLINIST_ID = P_CLIENT_VIOLINIST_INS_data?.RESULT?.VIOLINIST_ID;
    CLIENT_APP_VARIABLES.YN_SYSADMIN = P_CLIENT_VIOLINIST_INS_data?.RESULT?.YN_SYSADMIN;
    CLIENT_APP_VARIABLES.USER_DISPLAY_NAME = P_CLIENT_VIOLINIST_INS_data?.RESULT?.USER_DISPLAY_NAME;

    await SecureStore.setItemAsync('VIOLINIST_ID', String(CLIENT_APP_VARIABLES.VIOLINIST_ID));
  } catch {}

  //console.log('>>> REGISTER FUNC IS RUNNING <<<');
  console.log('CLIENT_APP_VARIABLES.VIOLINIST_ID:' + CLIENT_APP_VARIABLES.VIOLINIST_ID);
  DEBUG_CONSOLE_LOG();

}
