// SCREEN_MAIN_1_RECORDING_PARAMETERS.js
import { router } from 'expo-router';
import { useEffect, useState } from 'react';
import {
  Dimensions,
  FlatList,
  Modal,
  Pressable,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import { DEBUG_CONSOLE_LOG } from './CLIENT_APP_FUNCTIONS';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_MAIN_1_RECORDING_PARAMETERS({ density = 'ultra-compact' }) {
  const ultra = density === 'ultra-compact';

  const [songName, setSongName] = useState(CLIENT_APP_VARIABLES.SONG_NAME || '');
  const [dynamicParams, setDynamicParams] = useState([]);
  const [dropdownOptions, setDropdownOptions] = useState({});
  const [activeDropdown, setActiveDropdown] = useState(null);

  const screenWidth = Dimensions.get('window').width;

  // ~50% compact scale
  const FS_LABEL = ultra ? Math.max(11, screenWidth * 0.028) : Math.max(13, screenWidth * 0.035);
  const FS_VALUE = ultra ? 13 : 17;
  const CONTROL_H = ultra ? 24 : 50;
  const PAD_H = ultra ? 4 : 10;
  const PAD_V = ultra ? 3 : 8;
  const GRID_GAP = ultra ? 4 : 10;
  const GRID_MB = ultra ? 4 : 8;

  const mode = String(CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE || '').toUpperCase();
  const isCompose = mode === 'COMPOSE';
  const isPlay = mode === 'PLAY';
  const isPractice = mode === 'PRACTICE';

  useEffect(() => {
    // Re-seed local song state when mode changes
    setSongName(CLIENT_APP_VARIABLES.SONG_NAME || '');
    if (isCompose) {
      CLIENT_APP_VARIABLES.SONG_ID = null;
      CLIENT_APP_VARIABLES.RECORDING_ID = null;
      if (!CLIENT_APP_VARIABLES.SONG_NAME) {
        CLIENT_APP_VARIABLES.SONG_NAME = `New Composition on ${new Date().toLocaleString()}`;
        setSongName(CLIENT_APP_VARIABLES.SONG_NAME);
      }
    } else if (isPlay || isPractice) {
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    }
    fetchParameterNames();
    DEBUG_CONSOLE_LOG();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE]);

  const fetchParameterNames = async () => {
    const { VIOLINIST_ID, COMPOSE_PLAY_OR_PRACTICE, SONG_ID, RECORDING_ID } = CLIENT_APP_VARIABLES;
    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_DD_PARAMETER_NAMES',
          PARAMS: { VIOLINIST_ID, COMPOSE_PLAY_OR_PRACTICE, SONG_ID, RECORDING_ID },
        }),
      });
      const json = await res.json();
      const results = json.RESULT || [];
      const dropdownFetches = {};
      results.forEach((param) => {
        if (param.APP_VARIABLE_NAME && param.PARAMETER_VALUE !== undefined) {
          CLIENT_APP_VARIABLES[param.APP_VARIABLE_NAME] = param.PARAMETER_VALUE;
        }
        if (param.PARAMETER_SELECTION_TYPE === 'drop-down') {
          dropdownFetches[param.PARAMETER_NAME] = fetchDropdownOptions(param.PARAMETER_NAME);
        }
      });
      setDynamicParams(results);
      await Promise.all(Object.values(dropdownFetches));
    } catch (err) {
      console.error('Error fetching parameter names:', err);
    }
  };

  const fetchDropdownOptions = async (parameterName) => {
    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_DD_PARAMETER_VALUE',
          PARAMS: { VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID, PARAMETER_NAME: parameterName },
        }),
      });
      const json = await res.json();
      setDropdownOptions((prev) => ({ ...prev, [parameterName]: json.RESULT || [] }));
    } catch (err) {
      console.error('Error fetching dropdown for ' + parameterName + ':', err);
    }
  };

  // Helpers
  const same = (a, b) => String(a) === String(b);

  const isNoteParam = (name = '') => {
    const n = name.toUpperCase();
    return n.includes('NOTE') && (n.includes('FASTEST') || n.includes('SHORTEST'));
  };

  // Normalize note labels to guaranteed Unicode (in case backend sends blank/wrong)
  const normalizeNoteGlyph = (value, display) => {
    const v = String(value || '').toLowerCase();
    if (v.includes('16')) return '\uD834\uDD61'; // 𝅘𝅥𝅯
    if (v.includes('32')) return '\uD834\uDD62'; // 𝅘𝅥𝅰
    // Quarter/8th usually fine as ♩ ♪
    return display;
  };

  // Width rules to pack more per row
  const widthPctForParam = (name) => {
    const n = (name || '').toUpperCase();
    if (n.includes('BPM')) return '18%';
    if (n.includes('TIME SIGNATURE') || n.includes('TIMING')) return '20%';
    if (n.includes('TUNING')) return '26%';
    if (n.includes('SHARE')) return '26%';
    if (n.includes('GOAL TARGET') || n === 'GOAL') return '40%';
    if (n.includes('DOUBLE') || n.includes('SUPER') || n.includes('FASTEST') || n.includes('SHORTEST')) return '26%';
    return '28%';
  };

  const renderDropdown = (param, index) => {
    const options = dropdownOptions[param.PARAMETER_NAME] || [];

    // Resolve selected label robustly
    const fromOptions = options.find((o) => same(o.PARAMETER_VALUE, param.PARAMETER_VALUE));
    const upperName = (param.PARAMETER_NAME || '').toUpperCase();
    const noteParam = isNoteParam(upperName);

    // Selected text with fallbacks
    const rawSelectedLabel =
      (fromOptions && fromOptions.PARAMETER_DISPLAY_VALUE) ||
      param.PARAMETER_DISPLAY_VALUE ||
      param.PARAMETER_VALUE ||
      '';

    const selectedLabel = noteParam
      ? normalizeNoteGlyph(param.PARAMETER_VALUE, rawSelectedLabel)
      : rawSelectedLabel;

    const labelStyle = [
      styles.dropdownText,
      { fontSize: FS_VALUE },
      noteParam ? { fontFamily: 'NotoMusic' } : null, // font loaded globally in _layout
    ];

    return (
      <>
        <TouchableOpacity
          style={[styles.dropdownBase, { minHeight: CONTROL_H, paddingHorizontal: PAD_H, paddingVertical: PAD_V }]}
          onPress={() => setActiveDropdown(index)}
        >
          <View style={[styles.dropdownRow, { gap: 4 }]}>
            <Text
              style={labelStyle}
              numberOfLines={1}
              adjustsFontSizeToFit
              minimumFontScale={0.6}
              ellipsizeMode="tail"
            >
              {selectedLabel || 'Select'}
            </Text>
            <Text style={styles.dropdownIcon}>▼</Text>
          </View>
        </TouchableOpacity>

        {activeDropdown === index && (
          <Modal transparent animationType="fade">
            <Pressable style={styles.modalBackdrop} onPress={() => setActiveDropdown(null)} />
            <View style={styles.modal}>
              <FlatList
                data={options}
                keyExtractor={(item) => String(item.PARAMETER_VALUE)}
                renderItem={({ item }) => {
                  const display = noteParam
                    ? normalizeNoteGlyph(item.PARAMETER_VALUE, item.PARAMETER_DISPLAY_VALUE)
                    : item.PARAMETER_DISPLAY_VALUE;
                  return (
                    <TouchableOpacity
                      style={styles.modalOption}
                      onPress={() => {
                        const updated = [...dynamicParams];
                        updated[index].PARAMETER_VALUE = item.PARAMETER_VALUE;
                        updated[index].PARAMETER_DISPLAY_VALUE = item.PARAMETER_DISPLAY_VALUE;
                        setDynamicParams(updated);
                        if (param.APP_VARIABLE_NAME) {
                          CLIENT_APP_VARIABLES[param.APP_VARIABLE_NAME] = item.PARAMETER_VALUE;
                        }
                        setActiveDropdown(null);
                      }}
                    >
                      <Text
                        style={[
                          { fontSize: FS_VALUE },
                          noteParam ? { fontFamily: 'NotoMusic' } : null,
                        ]}
                      >
                        {display}
                      </Text>
                    </TouchableOpacity>
                  );
                }}
              />
            </View>
          </Modal>
        )}
      </>
    );
  };

  return (
    <ScrollView
      style={[
        styles.container,
        {
          paddingTop: (StatusBar.currentHeight || 30),
          paddingBottom: ultra ? 0 : 2,
          paddingHorizontal: ultra ? 8 : 10,
        },
      ]}
      keyboardShouldPersistTaps="handled"
    >
      {/* Song field — varies by mode */}
      <View style={[styles.paramBlock, { marginBottom: ultra ? 4 : 6 }]}>
        <Text style={[styles.label, { fontSize: FS_LABEL, marginBottom: ultra ? 2 : 4 }]}>Song:</Text>

        {/* COMPOSE: editable input (local state; write back on blur) */}
        {isCompose && (
          <View style={[styles.inputWrap, { minHeight: CONTROL_H, paddingHorizontal: PAD_H, paddingVertical: PAD_V }]}>
            <TextInput
              style={[styles.inputText, { fontSize: FS_VALUE }]}
              value={songName}
              onChangeText={setSongName}
              onBlur={() => { CLIENT_APP_VARIABLES.SONG_NAME = songName; }}
              placeholder="Type a song name"
              numberOfLines={1}
              adjustsFontSizeToFit
              minimumFontScale={0.7}
              autoCorrect={false}
              autoCapitalize="none"
            />
          </View>
        )}

        {/* PLAY: dropdown (opens search) */}
        {isPlay && (
          <TouchableOpacity
            style={[styles.dropdownBase, { minHeight: CONTROL_H, paddingHorizontal: PAD_H, paddingVertical: PAD_V }]}
            onPress={() => router.push('/SCREEN_SONG_SEARCH')}
          >
            <View style={[styles.dropdownRow, { gap: 4 }]}>
              <Text
                style={[styles.dropdownText, { fontSize: FS_VALUE }]}
                numberOfLines={1}
                adjustsFontSizeToFit
                minimumFontScale={0.6}
                ellipsizeMode="tail"
              >
                {CLIENT_APP_VARIABLES.SONG_NAME || 'Select a Song'}
              </Text>
              <Text style={styles.dropdownIcon}>▼</Text>
            </View>
          </TouchableOpacity>
        )}

        {/* PRACTICE: read-only, not a dropdown */}
        {isPractice && (
          <View style={[styles.dropdownBase, { minHeight: CONTROL_H, paddingHorizontal: PAD_H, paddingVertical: PAD_V }]}>
            <View style={[styles.dropdownRow, { gap: 4 }]}>
              <Text
                style={[styles.dropdownText, { fontSize: FS_VALUE }]}
                numberOfLines={1}
                adjustsFontSizeToFit
                minimumFontScale={0.6}
                ellipsizeMode="tail"
              >
                {CLIENT_APP_VARIABLES.SONG_NAME || 'Song'}
              </Text>
            </View>
          </View>
        )}
      </View>

      {/* Dynamic parameter grid */}
      <View style={[styles.paramGrid, { gap: GRID_GAP }]}>
        {dynamicParams.map((param, index) => (
          <View key={index} style={[styles.gridItem, { width: widthPctForParam(param.PARAMETER_NAME), marginBottom: GRID_MB }]}>
            <Text style={[styles.gridLabel, { fontSize: Math.max(10.5, FS_LABEL * 0.9), marginBottom: 2 }]}>
              {param.PARAMETER_DISPLAY_NAME}
            </Text>
            {param.PARAMETER_SELECTION_TYPE === 'drop-down' ? (
              renderDropdown(param, index)
            ) : (
              <TextInput
                style={[
                  styles.input,
                  { height: CONTROL_H, paddingHorizontal: PAD_H, paddingVertical: PAD_V, fontSize: FS_VALUE },
                ]}
                value={String(param.PARAMETER_VALUE || '')}
                onChangeText={(text) => {
                  const updated = [...dynamicParams];
                  updated[index].PARAMETER_VALUE = text;
                  updated[index].PARAMETER_DISPLAY_VALUE = text;
                  setDynamicParams(updated);
                  if (param.APP_VARIABLE_NAME) {
                    CLIENT_APP_VARIABLES[param.APP_VARIABLE_NAME] = text;
                  }
                }}
                placeholder={'Enter ' + param.PARAMETER_DISPLAY_NAME.toLowerCase()}
                numberOfLines={1}
                adjustsFontSizeToFit
                minimumFontScale={0.7}
              />
            )}
          </View>
        ))}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {},
  paramBlock: {},
  label: { fontWeight: 'bold' },

  dropdownBase: {
    borderWidth: 1,
    borderRadius: 6,
    backgroundColor: '#f0f0f0',
    justifyContent: 'center',
    borderColor: '#ccc',
  },
  dropdownRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    flexWrap: 'nowrap',
  },
  dropdownText: { flex: 1, textAlign: 'left' },
  dropdownIcon: { marginLeft: 4 },

  // COMPOSE input shell (white to signal editability)
  inputWrap: {
    borderWidth: 1,
    borderRadius: 6,
    borderColor: '#ccc',
    backgroundColor: '#fff',
    justifyContent: 'center',
  },
  inputText: { padding: 0 },

  input: {
    borderWidth: 1,
    borderRadius: 6,
    backgroundColor: '#fff',
    borderColor: '#ccc',
  },

  paramGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    justifyContent: 'flex-start',
  },
  gridItem: {},
  gridLabel: { fontWeight: 'bold' },

  modalBackdrop: {
    position: 'absolute',
    top: 0, left: 0, right: 0, bottom: 0,
    backgroundColor: 'rgba(0,0,0,0.3)',
  },
  modal: {
    position: 'absolute',
    top: '30%',
    left: '10%',
    right: '10%',
    backgroundColor: 'white',
    borderRadius: 8,
    padding: 16,
    maxHeight: 300,
  },
  modalOption: {
    padding: 10,
    borderBottomWidth: 1,
    borderBottomColor: '#ccc',
  },
});
