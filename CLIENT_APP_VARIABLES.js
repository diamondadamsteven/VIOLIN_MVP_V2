// CLIENT_APP_VARIABLES.js

const CLIENT_APP_VARIABLES = {
  VIOLINIST_ID: null,
  YN_SYSADMIN: null,
  USER_DISPLAY_NAME: null,
  // Parameters in registration/login page
  DEVICE_ID: null,
  IP_ADDRESS: null,
  LATITUDE: null,
  LONGITUDE: null,

  BACKEND_URL: 'http://192.168.1.159:8000',  // Replace with your PC's IP
  COMPOSE_PLAY_OR_PRACTICE: null,
  // Populated by Song or Recording Search page
  SONG_ID: null,
  RECORDING_ID: null, //Recording only
  SHARE_LEVEL: null, 
  SHARE_WITH_VIOLINIST_ID: null,
  AUDIO_STREAM_FILE_NAME: null,
  SONG_NAME: null,
  BPM: null,
  GOAL_TARGET: null,
  GOAL_TARGET_RECORDING_ID: null,
  TUNING: null,
  FASTEST_NOTE_IN_BEATS: null,
  YN_HAS_DOUBLE_STOPS: null,
  YN_HAS_HIGH_NOTES: null,
  TIME_SIGNATURE: null,
  BREAKDOWN_NAME: null,
  YN_SHOW_NOTE_DETAILS: null,
  YN_SHOW_ADVANCED: null,
  COUNTDOWN_BEATS: null,
  CONDUCTOR_MESSAGE_TEXT: null,
  YN_STOP_RECORDING: null,
  YN_REFRESH_NEXT_AUDIO_CHUNK: null,
  START_AUDIO_CHUNK_NO: null,
  END_AUDIO_CHUNK_NO: null,
  NOTE_ORDER_NO: null,
  COMPOSE_CHUNK_MINIMUM_DURATION_IN_MS: null,
	CONDUCTOR_MESSAGE_DISPLAY_FOR_DURATION_IN_MS: null,
	CONDUCTOR_MOOD_GOOD_BAD_OR_NEUTRAL: null

  // Add more variables as needed...
};

export default CLIENT_APP_VARIABLES;
