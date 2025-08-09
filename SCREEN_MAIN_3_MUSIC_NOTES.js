// SCREEN_MAIN_3_MUSIC_NOTES.js
import { forwardRef, useImperativeHandle, useRef, useState } from 'react';
import { Dimensions, PixelRatio, View } from 'react-native';
import { WebView } from 'react-native-webview';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

const SCREEN_MAIN_3_MUSIC_NOTES = forwardRef((props, ref) => {
  const webViewRef = useRef(null);
  const [lastFetchedHash, setLastFetchedHash] = useState('');
  const [webViewHeight, setWebViewHeight] = useState(200); // Initial height

  const {
    VIOLINIST_ID,
    SONG_ID,
    RECORDING_ID,
    START_AUDIO_CHUNK_NO,
    END_AUDIO_CHUNK_NO,
    BREAKDOWN_NAME,
    BACKEND_URL,
  } = CLIENT_APP_VARIABLES;

  const REFRESH = async () => {
    // Ensure device pixel info
    const logicalWidth = Dimensions.get('window').width;
    const pixelWidth = Math.floor(PixelRatio.getPixelSizeForLayoutSize(logicalWidth));
    const devicePixelRatio = PixelRatio.get();

    CLIENT_APP_VARIABLES.SCREEN_WIDTH_IN_PIXELS = pixelWidth;
    CLIENT_APP_VARIABLES.DEVICE_PIXEL_RATIO = devicePixelRatio;

    if (!SONG_ID && !RECORDING_ID) return;

    // Ensure a valid breakdown for hashing/fetch
    if (!CLIENT_APP_VARIABLES.BREAKDOWN_NAME) {
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    }

    const effectiveBreakdown = CLIENT_APP_VARIABLES.BREAKDOWN_NAME;
    const hash = `${SONG_ID}-${RECORDING_ID}-${START_AUDIO_CHUNK_NO}-${effectiveBreakdown}`;
    if (hash === lastFetchedHash) return;

    try {
      const res = await fetch(`${BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_SONG_NOTES_VEXFLOW_GET',
          PARAMS: {
            VIOLINIST_ID,
            SONG_ID,
            RECORDING_ID,
            START_AUDIO_CHUNK_NO,
            END_AUDIO_CHUNK_NO,
            BREAKDOWN_NAME: effectiveBreakdown,
            SCREEN_WIDTH_IN_PIXELS: pixelWidth,
            DEVICE_PIXEL_RATIO: devicePixelRatio,
          },
        }),
      });

      const json = await res.json();
      if (json?.RESULT?.length) {
        console.log(`🎼 VEXFLOW_CMD_COUNT: ${json.RESULT.length}`);
        setLastFetchedHash(hash);

        // If WebView hasn't mounted yet, skip
        if (!webViewRef.current) return;

        setTimeout(() => {
          webViewRef.current?.postMessage(
            JSON.stringify({ type: 'renderCommands', payload: json.RESULT })
          );
        }, 500);
      } else {
        console.log('🚫 NO_COMMANDS_RETURNED');
      }
    } catch (err) {
      console.error('❌ ERROR_FETCHING_COMMANDS:', err);
    }
  };

  // Expose REFRESH() to parent
  useImperativeHandle(ref, () => ({ REFRESH }));

  return (
    <View style={{ width: '100%', height: webViewHeight }}>
      <WebView
        ref={webViewRef}
        originWhitelist={['*']}
        javaScriptEnabled
        domStorageEnabled
        style={{ width: '100%', height: webViewHeight }}
        source={{ html: DISPLAY_MUSIC_NOTES() }}
        onMessage={(event) => {
          const msg = event.nativeEvent.data;
          if (msg.startsWith('🎻 WEBVIEW_HEIGHT:')) {
            const height = parseFloat(msg.split(':')[1]);
            if (!isNaN(height)) {
              const scaledHeight = height / CLIENT_APP_VARIABLES.DEVICE_PIXEL_RATIO + 20; // padding
              setWebViewHeight(scaledHeight);
              console.log(`🎻 HEIGHT UPDATED: ${scaledHeight}`);
            }
          } else {
            console.log('🎻 WEBVIEW_MSG:', msg);
          }
        }}
      />
    </View>
  );
});

export default SCREEN_MAIN_3_MUSIC_NOTES;

function DISPLAY_MUSIC_NOTES() {
  return `
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <script src="https://unpkg.com/vexflow@4.1.0/build/cjs/vexflow.js"></script>
  </head>
  <body style="margin:0;padding:0;">
    <div id="output" style="width: 100vw; overflow-x: hidden;"></div>
    <script>
      const { Flow } = Vex;
      const div = document.getElementById("output");
      const renderer = new Flow.Renderer(div, Flow.Renderer.Backends.SVG);
      let context = null;

      const stave = {};
      // 🔒 Safe NO-OP proxy for 'notes' so missing indices don't crash
      const notes = new Proxy([], {
        get(target, prop) {
          if (prop in target) return target[prop];
          return createNoopProxy(\`notes[\${String(prop)}]\`);
        },
        set(target, prop, value) {
          target[prop] = value;
          return true;
        }
      });

      // (Optional) proxies for other structures if needed later:
      const voice = {};
      let formatter = new Flow.Formatter();

      function rnLog(msg) {
        // Send to RN log as well as console for easier debugging
        try { console.log(msg); } catch(e) {}
        try { window.ReactNativeWebView?.postMessage("LOG: " + msg); } catch(e) {}
      }

      function createNoopProxy(path) {
        const fn = function() {};
        return new Proxy(fn, {
          get(_t, key) {
            if (key === Symbol.toPrimitive) return () => '';
            return createNoopProxy(\`\${path}.\${String(key)}\`);
          },
          apply(_t, _thisArg, args) {
            rnLog(\`⚠️ NO-OP call at \${path}(...) args=\${JSON.stringify(args).slice(0,200)}\`);
            return createNoopProxy(path);
          }
        });
      }

      function runCommands(commands) {
        try {
          for (let i = 0; i < commands.length; i++) {
            const cmdObj = commands[i];
            const cmd = cmdObj.VEXFLOW_COMMAND_TEXT;

            // ✅ Log each command before executing
            rnLog(\`🔧 Eval CMD #\${cmdObj.COMMAND_ORDER_NO}: \${cmd}\`);

            try {
              eval(cmd);
            } catch (err) {
              rnLog(\`❌ Eval error in CMD #\${cmdObj.COMMAND_ORDER_NO}: \${err.message} | CMD: \${cmd}\`);
              // keep going so other commands can render
              continue;
            }
          }

          const height = div.getBoundingClientRect().height / 2; // DIVIDE BY 2 ADDED BY ADAM
          window.ReactNativeWebView?.postMessage("🎻 WEBVIEW_HEIGHT:" + height);
          rnLog("✅ Notes rendered (with guards). height=" + height);
        } catch (err) {
          rnLog("❌ Top-level error: " + err.message);
        }
      }

      window.addEventListener("message", (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === "renderCommands") {
            runCommands(message.payload);
          }
        } catch (err) {
          rnLog("❌ JSON parse error: " + err.message);
        }
      });

      rnLog("🎻 WebView JS loaded (logging + guards enabled)");
    </script>
  </body>
</html>
`;
}
