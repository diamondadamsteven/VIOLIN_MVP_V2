// APP_VARIABLES.js

const CLIENT_APP_VARIABLES = {
  VIOLINIST_ID: null,
  DEVICE_ID: null,
  IP_ADDRESS: null,
  LATITUDE: null,
  LONGITUDE: null,
  BACKEND_URL: 'http://192.168.1.54:8000'  // Replace with your PC's IP
  // Add more variables as needed...
};

// export function CLIENT_APP_VARIABLES_SET(NAME, VALUE) {
//   if (NAME in AppVariables) {
//     AppVariables[NAME] = VALUE;
//   } else {
//     console.warn(`App variable ${NAME} is not declared in APP_VARIABLES.js`);
//   }
// }

// export function CLIENT_APP_VARIABLES_GET(NAME) {
//   return AppVariables[NAME];
// }

export default CLIENT_APP_VARIABLES;
