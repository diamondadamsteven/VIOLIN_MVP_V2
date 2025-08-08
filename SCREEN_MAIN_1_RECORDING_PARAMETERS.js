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

export default function SCREEN_MAIN_1_RECORDING_PARAMETERS() {
  const [recordingName, setRecordingName] = useState('');
  const [dynamicParams, setDynamicParams] = useState([]);
  const [dropdownOptions, setDropdownOptions] = useState({});
  const [activeDropdown, setActiveDropdown] = useState(null);

  const screenWidth = Dimensions.get('window').width;
  const fontSize = screenWidth * 0.035;

  useEffect(() => {
    const mode = CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE;

    if (mode === 'COMPOSE') {
      CLIENT_APP_VARIABLES.SONG_ID = null;
      CLIENT_APP_VARIABLES.RECORDING_ID = null;
    } else if (mode === 'PLAY' || mode === 'PRACTICE') {
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    }

    fetchParameterNames();
    DEBUG_CONSOLE_LOG();
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

      results.forEach(param => {
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
          PARAMS: {
            VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
            PARAMETER_NAME: parameterName,
          },
        }),
      });

      const json = await res.json();
      setDropdownOptions(prev => ({
        ...prev,
        [parameterName]: json.RESULT || [],
      }));
    } catch (err) {
      console.error('Error fetching dropdown for ' + parameterName + ':', err);
    }
  };

  const renderDropdown = (param, index) => {
    const options = dropdownOptions[param.PARAMETER_NAME] || [];
    const selectedLabel = options.find(opt => opt.PARAMETER_VALUE === param.PARAMETER_VALUE)?.PARAMETER_DISPLAY_VALUE;
    const isMultiLine = param.PARAMETER_NAME === 'SONG_NAME' || param.PARAMETER_NAME === 'RECORDING_NAME';

    return (
      <>
        <TouchableOpacity
          style={[styles.dropdownCompact, isMultiLine && styles.multiLineDropdown]}
          onPress={() => setActiveDropdown(index)}
        >
          <View style={styles.dropdownRow}>
            <Text style={styles.dropdownText} numberOfLines={isMultiLine ? 0 : 1}>
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
                keyExtractor={(item) => item.PARAMETER_VALUE}
                renderItem={({ item }) => (
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
                    <Text>{item.PARAMETER_DISPLAY_VALUE}</Text>
                  </TouchableOpacity>
                )}
              />
            </View>
          </Modal>
        )}
      </>
    );
  };

  return (
    <ScrollView style={[styles.container, { paddingTop: (StatusBar.currentHeight || 30) + 4 }]}>
      <View style={styles.paramBlock}>
        <Text style={[styles.label, { fontSize }]}>Song:</Text>
        <TouchableOpacity
          style={[styles.dropdownCompact, styles.multiLineDropdown]}
          onPress={() => router.push('/SCREEN_SONG_SEARCH')}
        >
          <View style={styles.dropdownRow}>
            <Text style={styles.dropdownText} numberOfLines={0}>
              {CLIENT_APP_VARIABLES.SONG_NAME || 'Select a Song'}
            </Text>
            <Text style={styles.dropdownIcon}>▼</Text>
          </View>
        </TouchableOpacity>
      </View>

      {CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE === 'PLAY' &&
        CLIENT_APP_VARIABLES.RECORDING_ID && (
          <View style={styles.paramBlock}>
            <Text style={[styles.label, { fontSize }]}>Recording:</Text>
            <View style={[styles.dropdownCompact, styles.multiLineDropdown]}>
              <View style={styles.dropdownRow}>
                <Text style={styles.dropdownText} numberOfLines={0}>
                  {recordingName}
                </Text>
                <Text style={styles.dropdownIcon}>▼</Text>
              </View>
            </View>
          </View>
        )}

      <View style={styles.paramGrid}>
        {dynamicParams.map((param, index) => (
          <View key={index} style={styles.gridItem}>
            <Text style={[styles.gridLabel, { fontSize: fontSize * 0.9 }]}>
              {param.PARAMETER_DISPLAY_NAME}
            </Text>
            {param.PARAMETER_SELECTION_TYPE === 'drop-down'
              ? renderDropdown(param, index)
              : (
                <TextInput
                  style={styles.input}
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
                />
              )}
          </View>
        ))}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    paddingHorizontal: 10,
    paddingBottom: 4,
  },
  paramBlock: {
    marginBottom: 6,
  },
  label: {
    fontWeight: 'bold',
    marginBottom: 4,
  },
  dropdownCompact: {
    borderWidth: 1,
    padding: 8,
    borderRadius: 4,
    backgroundColor: '#f0f0f0',
    justifyContent: 'center',
  },
  multiLineDropdown: {
    minHeight: 40,
  },
  dropdownRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    flexWrap: 'wrap',
  },
  dropdownText: {
    flex: 1,
    textAlign: 'left',
    flexWrap: 'wrap',
  },
  dropdownIcon: {
    marginLeft: 4,
  },
  input: {
    borderWidth: 1,
    padding: 8,
    borderRadius: 4,
    backgroundColor: '#fff',
  },
  paramGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 10,
    justifyContent: 'space-between',
  },
  gridItem: {
    width: '47%',
    marginBottom: 8,
  },
  gridLabel: {
    fontWeight: 'bold',
    marginBottom: 2,
  },
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
