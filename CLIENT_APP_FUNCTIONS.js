// CLIENT_APP_FUNCTIONS.js
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export function DEBUG_CONSOLE_LOG_GET_CALLER_DETAILS(depth = 2) {
  try {
    const stack = new Error().stack.split('\n');
    const targetLine = stack[2 + (depth - 1)] || '';

    const chromeMatch = targetLine.match(/at (\w+).* \((.*):(\d+):(\d+)\)/);
    const firefoxMatch = targetLine.match(/(\w+)@(.*):(\d+):(\d+)/);

    const match = chromeMatch || firefoxMatch;

    if (match) {
      const [, functionName, fileName, line, column] = match;
      return {
        functionName,
        fileName,
        line: Number(line),
        column: Number(column),
      };
    }
  } catch {}

  return {
    functionName: 'unknown',
    fileName: 'unknown',
    line: -1,
    column: -1,
  };
}

export function DEBUG_CONSOLE_LOG(depth = 2) {
  const { functionName, fileName, line } = DEBUG_CONSOLE_LOG_GET_CALLER_DETAILS(depth);

  const now = new Date();
  const timestamp = now.toLocaleString(); // or use toISOString()

  console.log(
    `[${timestamp}] ${functionName} (${fileName}:${line}) APP VARS:\n` +
    JSON.stringify(CLIENT_APP_VARIABLES, null, 2)
  );
}
