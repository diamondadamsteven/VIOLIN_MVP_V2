import { useRef, useState } from 'react';
import { Button, ScrollView, StyleSheet, Text, TextInput, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function WSConnectivityTest() {
  const [url, setUrl] = useState(() => {
    try {
      const base = String(CLIENT_APP_VARIABLES.BACKEND_URL || '').replace(/\/+$/, '');
      const u = new URL(base);
      const proto = u.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${proto}//${u.hostname}:7070/ws/echo`;
    } catch {
      return 'ws://192.168.1.___:7070/ws/echo'; // put your PC IP
    }
  });
  const [status, setStatus] = useState('disconnected');
  const [log, setLog] = useState([]);
  const wsRef = useRef(null);
  const pingRef = useRef(null);

  const append = (line) => setLog((prev) => [...prev, `${new Date().toLocaleTimeString()}  ${line}`]);

  const connect = () => {
    try {
      append(`connecting to ${url}`);
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        append('onopen');
        // send a ping every 2s so we see roundtrips
        pingRef.current = setInterval(() => {
          try { ws.send(JSON.stringify({ type: 'ping', t: Date.now() })); } catch {}
        }, 2000);
      };

      ws.onmessage = (evt) => {
        let txt = '';
        if (typeof evt.data === 'string') txt = evt.data;
        else {
          try { txt = new TextDecoder().decode(evt.data); } catch { txt = '[binary message]'; }
        }
        append(`onmessage: ${txt}`);
      };

      ws.onerror = (e) => {
        append(`onerror: ${e?.message || 'unknown error'}`);
      };

      ws.onclose = () => {
        setStatus('closed');
        append('onclose');
        if (pingRef.current) { clearInterval(pingRef.current); pingRef.current = null; }
      };
    } catch (e) {
      append(`connect error: ${e?.message || e}`);
    }
  };

  const sendHello = () => {
    try { wsRef.current?.send('hello'); append('sent: hello'); } catch {}
  };

  const close = () => {
    try { wsRef.current?.close(); } catch {}
  };

  return (
    <View style={S.wrap}>
      <Text style={S.h1}>WS Echo Connectivity Test</Text>
      <TextInput style={S.input} value={url} onChangeText={setUrl} autoCapitalize="none" />
      <View style={S.row}>
        <Button title="Connect" onPress={connect} />
        <Button title="Send hello" onPress={sendHello} />
        <Button title="Close" onPress={close} />
      </View>
      <Text>Status: {status}</Text>
      <ScrollView style={S.log}>
        {log.map((l, i) => <Text key={i} style={S.line}>{l}</Text>)}
      </ScrollView>
    </View>
  );
}

const S = StyleSheet.create({
  wrap: { padding: 12, gap: 8, flex: 1, backgroundColor: 'white' },
  h1: { fontSize: 18, fontWeight: '600' },
  input: { borderWidth: 1, borderColor: '#ccc', borderRadius: 6, padding: 8 },
  row: { flexDirection: 'row', gap: 8 },
  log: { flex: 1, marginTop: 8, borderWidth: 1, borderColor: '#eee', borderRadius: 6, padding: 8 },
  line: { fontFamily: 'monospace' },
});
