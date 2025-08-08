// SCREEN_TEST_VEXFLOW.js
import { View } from 'react-native';
import { WebView } from 'react-native-webview';

export default function SCREEN_TEST_VEXFLOW() {
  const htmlContent = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>VexFlow Test</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vexflow/4.1.0/vexflow-min.js"></script>
  <style>
    body { margin: 0; padding: 0; }
    canvas { border: 1px solid black; display: block; margin: auto; }
  </style>
</head>
<body>
  <div id="output"></div>
  <script>
    window.ReactNativeWebView?.postMessage("🧪 WebView JS loaded");

    document.addEventListener("DOMContentLoaded", function() {
      try {
        const VF = Vex.Flow;
        window.ReactNativeWebView?.postMessage("✅ VexFlow loaded");

        const div = document.getElementById("output");
        const canvas = document.createElement("canvas");
        canvas.width = 320;
        canvas.height = 160;
        div.appendChild(canvas);

        const renderer = new VF.Renderer(canvas, VF.Renderer.Backends.CANVAS);
        const context = renderer.getContext();

        const stave = new VF.Stave(10, 40, 300);
        stave.addClef("treble").setContext(context).draw();

        const note = new VF.StaveNote({ clef: "treble", keys: ["c/4"], duration: "q" });

        const voice = new VF.Voice({ num_beats: 1, beat_value: 4 });
        voice.addTickables([note]);

        new VF.Formatter().joinVoices([voice]).format([voice], 250);
        voice.draw(context, stave);

        window.ReactNativeWebView?.postMessage("✅ Drawing complete");
      } catch (e) {
        window.ReactNativeWebView?.postMessage("❌ Drawing error: " + e.message);
      }
    });
  </script>
</body>
</html>
`;

  return (
    <View style={{ height: 200 }}>  {/* Set a fixed height */}
      <WebView
        originWhitelist={['*']}
        javaScriptEnabled
        domStorageEnabled
        source={{ html: htmlContent }}
        onMessage={(event) => {
          console.log('📩 WebView message:', event.nativeEvent.data);
        }}
      />
    </View>
  );
}
