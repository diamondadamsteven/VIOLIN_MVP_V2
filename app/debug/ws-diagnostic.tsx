// app/debug/ws-diagnostic.tsx
import { useRouter } from 'expo-router';
import React, { useEffect, useRef, useState } from 'react';
import { Button, ScrollView, Text, TextInput, View } from 'react-native';
import CLIENT_APP_VARIABLES from '../../CLIENT_APP_VARIABLES';

function getHostFromBackend() {
  try {
    const base = String(CLIENT_APP_VARIABLES.BACKEND_URL || '').replace(/\/+$/, '');
    const u = new URL(base);
    return u.hostname;
  } catch {
    return null;
  }
}

export default function WSDiagnostic() {
  const router = useRouter();
  const [health, setHealth] = useState<string>('(not checked)');
  const [wsState, setWsState] = useState<'closed'|'open'|'error'|'connecting'>('closed');
  const [log, setLog] = useState<string[]>([]);
  const [msg, setMsg] = useState('hello');
  const wsRef = useRef<WebSocket | null>(null);

  const host = getHostFromBackend();
  const httpProto = CLIENT_APP_VARIABLES.BACKEND_URL?.startsWith('https') ? 'https:' : 'http:';
  const wsProto   = CLIENT_APP_VARIABLES.BACKEND_URL?.startsWith('https') ? 'wss:'  : 'ws:';
  const healthUrl = host ? `${httpProto}//${host}:7070/health` : null;
  const echoUrl   = host ? `${wsProto}//${host}:7070/ws/echo`   : null;

  function push(line: string) {
    setLog(prev => [...prev.slice(-200), line]);
  }

  async function checkHealth() {
    if (!healthUrl) { setHealth('invalid BACKEND_URL'); return; }
    try {
      const r = await fetch(healthUrl);
      const t = await r.text();
      setHealth(`${r.status}: ${t}`);
      push(`GET /health => ${r.status}`);
    } catch (e:any) {
      setHealth(`error: ${String(e?.message || e)}`);
      push(`GET /health error: ${String(e?.message || e)}`);
    }
  }

  function openWS() {
    if (!echoUrl) { push('Invalid echo URL'); return; }
    closeWS();
    setWsState('connecting');
    const ws = new WebSocket(echoUrl);
    wsRef.current = ws;

    ws.onopen = () => { setWsState('open'); push('WS open'); };
    ws.onerror = (e:any) => { setWsState('error'); push(`WS error: ${String(e?.message || e)}`); };
    ws.onclose = (e:any) => { setWsState('closed'); push(`WS close (code=${e?.code} reason=${e?.reason})`); };
    ws.onmessage = (evt:any) => {
      push(`WS message: ${typeof evt.data === 'string' ? evt.data : '<binary>'}`);
    };
  }

  function closeWS() {
    try { wsRef.current?.close(); } catch {}
    wsRef.current = null;
    setWsState('closed');
  }

  function sendMsg() {
    if (wsRef.current && wsState === 'open') {
      wsRef.current.send(msg);
      push(`sent: ${msg}`);
    }
  }

  useEffect(() => () => closeWS(), []);

  return (
    <View style={{ flex: 1, padding: 16, gap: 12 }}>
      <Text style={{ fontSize: 18, fontWeight: '600' }}>WS Diagnostic</Text>
      <Text>BACKEND_URL: {CLIENT_APP_VARIABLES.BACKEND_URL}</Text>
      <Text>Health URL: {healthUrl ?? '(invalid)'} </Text>
      <Text>Echo URL: {echoUrl ?? '(invalid)'} </Text>

      <Button title="Check /health" onPress={checkHealth} />
      <Text>Health: {health}</Text>

      <View style={{ flexDirection: 'row', gap: 12, marginTop: 8 }}>
        <Button title="Open WS" onPress={openWS} />
        <Button title="Close WS" color="#aa0000" onPress={closeWS} />
      </View>
      <Text>WS State: {wsState}</Text>

      <View style={{ flexDirection: 'row', gap: 8, alignItems: 'center' }}>
        <TextInput
          value={msg}
          onChangeText={setMsg}
          placeholder="message"
          style={{ flex: 1, borderWidth: 1, padding: 8, borderRadius: 6 }}
        />
        <Button title="Send" onPress={sendMsg} />
      </View>

      <Text style={{ marginTop: 8, fontWeight: '600' }}>Log</Text>
      <ScrollView style={{ flex: 1, borderWidth: 1, padding: 8, borderRadius: 6 }}>
        {log.map((l, i) => <Text key={i} style={{ fontFamily: 'monospace' }}>{l}</Text>)}
      </ScrollView>

      <Button title="Back" onPress={() => router.back()} />
    </View>
  );
}
