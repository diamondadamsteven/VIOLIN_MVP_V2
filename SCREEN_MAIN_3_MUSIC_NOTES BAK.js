import { useEffect, useRef, useState } from 'react';
import { View } from 'react-native';
import { WebView } from 'react-native-webview';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_MAIN_3_MUSIC_NOTES() {
  const webViewRef = useRef(null);
  const [lastFetchedHash, setLastFetchedHash] = useState('');

  const {
    VIOLINIST_ID,
    SONG_ID,
    RECORDING_ID,
    START_AUDIO_CHUNK_NO,
    END_AUDIO_CHUNK_NO,
    BREAKDOWN_NAME,
    BACKEND_URL,
    SCREEN_WIDTH_IN_PIXELS,
  } = CLIENT_APP_VARIABLES;

  const fetchNotes = async () => {
    if (!SONG_ID && !RECORDING_ID) return;

    const hash = `${SONG_ID}-${RECORDING_ID}-${START_AUDIO_CHUNK_NO}-${BREAKDOWN_NAME}`;
    if (hash === lastFetchedHash) return;

    try {
      const res = await fetch(`${BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_SONG_NOTES_GET',
          PARAMS: {
            VIOLINIST_ID,
            SONG_ID,
            RECORDING_ID,
            START_AUDIO_CHUNK_NO,
            END_AUDIO_CHUNK_NO,
            BREAKDOWN_NAME,
            SCREEN_WIDTH_IN_PIXELS,
          },
        }),
      });

      const json = await res.json();
      if (json?.RESULT?.length) {
        console.log(`NOTE_DATA_COUNT: ${json.RESULT.length}`);
        setLastFetchedHash(hash);

        setTimeout(() => {
          webViewRef.current?.postMessage(
            JSON.stringify({ type: 'renderNotes', data: json.RESULT })
          );
        }, 500);
      } else {
        console.log('NO_NOTES_RETURNED');
      }
    } catch (err) {
      console.error('ERROR_FETCHING_NOTES:', err);
    }
  };

  useEffect(() => {
    fetchNotes();
  }, [SONG_ID, RECORDING_ID, START_AUDIO_CHUNK_NO, BREAKDOWN_NAME]);

  return (
    <View style={{ height: 800 }}>
      <WebView
        ref={webViewRef}
        originWhitelist={['*']}
        javaScriptEnabled
        domStorageEnabled
        source={{ html: generateHTML() }}
        onMessage={(event) => {
          console.log('WEBVIEW_MSG:', event.nativeEvent.data);
        }}
      />
    </View>
  );
}

function generateHTML() {
  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Music Notation</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vexflow/4.1.0/vexflow-min.js"></script>
  <style>
    body { margin: 0; padding: 0; }
    svg { border: 1px solid black; display: block; margin: auto; }
  </style>
</head>
<body>
  <div id="output"></div>
  <script>
    window.ReactNativeWebView?.postMessage("WEBVIEW_LOADED");

    function rgbaOrBlack(rgba) {
      return rgba ? "rgba" + rgba : "rgba(0,0,0,1)";
    }

    function getDurationCode(beats) {
      if (beats >= 4.0) return "w";
      if (beats >= 2.0) return "h";
      if (beats >= 1.0) return "q";
      if (beats >= 0.5) return "8";
      if (beats >= 0.25) return "16";
      return "32";
    }

    function groupBy(array, key) {
      return array.reduce((result, item) => {
        const groupKey = String(item[key]);
        if (!result[groupKey]) result[groupKey] = [];
        result[groupKey].push(item);
        return result;
      }, {});
    }

    function drawNotes(noteData) {
      try {
        window.ReactNativeWebView?.postMessage("DRAW_NOTES_CALLED");
        const output = document.getElementById("output");
        output.innerHTML = "";

        const grouped = groupBy(noteData, 'STAFF_NO');
        const staffNos = Object.keys(grouped).map(Number).sort((a, b) => a - b);
        window.ReactNativeWebView?.postMessage("STAFF_COUNT: " + staffNos.length);

        staffNos.forEach(staffNo => {
          const notesForStaff = grouped[String(staffNo)];
          if (!notesForStaff || !notesForStaff.length) return;

          window.ReactNativeWebView?.postMessage("STAFF_" + staffNo + "_NOTE_COUNT: " + notesForStaff.length);

          const width = 600;
          const height = 100;
          const div = document.createElement("div");
          output.appendChild(div);

          const renderer = new Vex.Flow.Renderer(div, Vex.Flow.Renderer.Backends.SVG);
          const context = renderer.getContext();

          const stave = new Vex.Flow.Stave(10, 40, width - 20);
          if (staffNo === 1) stave.addClef("treble");
          stave.setContext(context).draw();

          const tickables = [];

          notesForStaff.forEach(n => {
            try {
              const isRest = n.YN_REST && n.YN_REST.toUpperCase() === "Y";
              const durationCode = getDurationCode(n.NUMBER_OF_BEATS);
              window.ReactNativeWebView?.postMessage(
                "NOTE STAFF_NO=" + staffNo + " X=" + n.HORIZONTAL_PIXEL_NO + " YN_REST=" + n.YN_REST + " DUR=" + durationCode
              );

              let note;
              if (isRest) {
                note = new Vex.Flow.StaveNote({
                  clef: "treble",
                  keys: ["b/4"],
                  duration: durationCode + "r",
                }).setStyle({ fillStyle: rgbaOrBlack(n.COLOR_RGBA_FOR_REST) });
              } else {
                const keys = [n.NOTE_1, n.NOTE_2, n.NOTE_3, n.NOTE_4].filter(Boolean);
                if (!keys.length) return;

                note = new Vex.Flow.StaveNote({
                  clef: "treble",
                  keys,
                  duration: durationCode,
                });

                keys.forEach((_, i) => {
                  const color = rgbaOrBlack(n["COLOR_RGBA_FOR_NOTE_" + (i + 1)]);
                  note.setKeyStyle(i, { fillStyle: color });
                });
              }

              if (n.NOTE_ORDER_NO) {
                note.attrs.el?.setAttribute?.("id", "note-" + n.NOTE_ORDER_NO);
              }

              tickables.push(note);
            } catch (noteErr) {
              window.ReactNativeWebView?.postMessage("ERROR_CREATING_NOTE: " + noteErr.message);
            }
          });

          try {
            const voice = new Vex.Flow.Voice({ num_beats: 4, beat_value: 4 });
            voice.addTickables(tickables);

            const formatter = new Vex.Flow.Formatter();
            formatter.joinVoices([voice]).format([voice], width - 40);
            voice.draw(context, stave);
          } catch (errVoice) {
            window.ReactNativeWebView?.postMessage("FORMATTER_ERROR: " + errVoice.message);
          }
        });

        window.ReactNativeWebView?.postMessage("RENDER_SUCCESS");
      } catch (err) {
        window.ReactNativeWebView?.postMessage("DRAW_NOTES_ERROR: " + err.message);
      }
    }

    function handleMessage(event) {
      try {
        const msg = JSON.parse(event.data);
        window.ReactNativeWebView?.postMessage("MSG_RECEIVED: " + msg.type);

        if (msg.type === "renderNotes" && msg.data) {
          window.ReactNativeWebView?.postMessage("NOTES_RECEIVED: " + msg.data.length);
          drawNotes(msg.data);
        } else {
          window.ReactNativeWebView?.postMessage("UNKNOWN_OR_MISSING_DATA");
        }
      } catch (e) {
        window.ReactNativeWebView?.postMessage("MSG_PARSE_ERROR: " + e.message);
      }
    }

    window.addEventListener("message", handleMessage);
    document.addEventListener("message", handleMessage);
  </script>
</body>
</html>
  `;
}
