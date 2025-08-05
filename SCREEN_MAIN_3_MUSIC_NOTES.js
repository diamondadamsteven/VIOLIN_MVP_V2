// SCREEN_MAIN_3_MUSIC_NOTES.js

import { useEffect, useRef, useState } from 'react';
import { View } from 'react-native';
import { WebView } from 'react-native-webview';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_MAIN_3_MUSIC_NOTES() {
  const webViewRef = useRef(null);
  const [noteData, setNoteData] = useState([]);
  const [lastFetchedHash, setLastFetchedHash] = useState('');

  const {
    VIOLINIST_ID,
    SONG_ID,
    RECORDING_ID,
    START_AUDIO_CHUNK_NO,
    END_AUDIO_CHUNK_NO,
    BREAKDOWN_NAME,
  } = CLIENT_APP_VARIABLES;

  const fetchNotes = async () => {
    if (!SONG_ID && !RECORDING_ID) return;

    const hash = `${SONG_ID}-${RECORDING_ID}-${START_AUDIO_CHUNK_NO}-${BREAKDOWN_NAME}`;
    if (hash === lastFetchedHash) return;

    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
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
          },
        }),
      });

      const json = await res.json();
      if (json?.length) {
        setNoteData(json);
        setLastFetchedHash(hash);

        // Send to WebView
        webViewRef.current?.postMessage(
          JSON.stringify({ type: 'renderNotes', data: json })
        );
      }
    } catch (err) {
      console.error('Error fetching notes:', err);
    }
  };

  useEffect(() => {
    fetchNotes();
  }, [SONG_ID, RECORDING_ID, START_AUDIO_CHUNK_NO, BREAKDOWN_NAME]);

  return (
    <View style={{ height: 600 }}>
      <WebView
        ref={webViewRef}
        originWhitelist={['*']}
        source={{ html: generateHTML() }}
        javaScriptEnabled
        domStorageEnabled
        onMessage={(event) => {
          console.log('WebView Message:', event.nativeEvent.data);
        }}
      />
    </View>
  );
}

// Generate a simple VexFlow HTML scaffold
function generateHTML() {
  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Music Notation</title>
  <script src="https://unpkg.com/vexflow/releases/vexflow-min.js"></script>
  <style>
    body { margin: 0; padding: 0; }
    canvas { width: 100%; }
  </style>
</head>
<body>
  <div id="output"></div>
  <script>
    const VF = Vex.Flow;

    function drawNotes(noteData) {
      const div = document.getElementById("output");
      div.innerHTML = '';  // Clear old content
      const renderer = new VF.Renderer(div, VF.Renderer.Backends.SVG);

      const width = window.innerWidth;
      const height = 600;
      renderer.resize(width, height);
      const context = renderer.getContext();

      const stave = new VF.Stave(10, 40, width - 20);
      stave.addClef("treble").setContext(context).draw();

      const notes = noteData.map((n) => {
        const keys = [n.NOTE_1 || "b/4"];
        const duration = (n.NUMBER_OF_BEATS == 2.0) ? "h" :
                         (n.NUMBER_OF_BEATS == 1.0) ? "q" :
                         (n.NUMBER_OF_BEATS == 0.5) ? "8" : "q";

        return new VF.StaveNote({
          clef: "treble",
          keys: keys,
          duration: duration
        });
      });

      const voice = new VF.Voice({ num_beats: 4, beat_value: 4 });
      voice.addTickables(notes);

      const formatter = new VF.Formatter().joinVoices([voice]).format([voice], width - 40);
      voice.draw(context, stave);
    }

    document.addEventListener('message', function(event) {
      const msg = JSON.parse(event.data);
      if (msg.type === 'renderNotes') {
        drawNotes(msg.data);
      }
    });
  </script>
</body>
</html>
`;
}
