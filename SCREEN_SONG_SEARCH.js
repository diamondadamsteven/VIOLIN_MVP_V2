// SCREEN_SONG_SEARCH.js
import { Picker } from '@react-native-picker/picker';
import { router } from 'expo-router';
import { useEffect, useState } from 'react';
import {
  Button,
  FlatList,
  Modal,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import { DEBUG_CONSOLE_LOG } from './CLIENT_APP_FUNCTIONS';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

let searchDebounce;

export default function SCREEN_SONG_SEARCH() {
  const [mode, setMode] = useState('Song');
  const [filter, setFilter] = useState('All');
  const [searchText, setSearchText] = useState('');
  const [results, setResults] = useState([]);
  const [sortField, setSortField] = useState(null);
  const [sortDirection, setSortDirection] = useState('asc');
  const [shareLevelOptions, setShareLevelOptions] = useState([]);
  const [violinistModalVisible, setViolinistModalVisible] = useState(false);
  const [violinistList, setViolinistList] = useState([]);
  const [selectedItemForShare, setSelectedItemForShare] = useState(null);

  useEffect(() => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
      fetchResults();
    }, 300);
    return () => clearTimeout(searchDebounce);
  }, [mode, filter, searchText]);

  function sortResults(data) {
    if (!sortField) return data;
    const sorted = [...data].sort((a, b) => {
      const aValue = a[sortField] ?? '';
      const bValue = b[sortField] ?? '';
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDirection === 'asc' ? aValue - bValue : bValue - aValue;
      } else {
        return sortDirection === 'asc'
          ? String(aValue).localeCompare(String(bValue))
          : String(bValue).localeCompare(String(aValue));
      }
    });
    return sorted;
  }

  async function fetchResults() {
    const spName = mode === 'Song' ? 'P_CLIENT_DD_SONG' : 'P_CLIENT_DD_RECORDING';
    const baseParams = {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      FILTER_TEXT: searchText || null,
      YN_YOUR_COMPOSITIONS_AND_UPLOADS_ONLY: null,
      YN_YOUR_REPERTOIRE_ONLY: null,
      YN_SONGS_SHARED_WITH_YOU_ONLY: null,
      YN_YOUR_RECORDINGS_ONLY: null,
      YN_RECORDINGS_SHARED_WITH_YOU_ONLY: null,
    };

    if (filter === 'Your Compositions/Uploads') {
      baseParams.YN_YOUR_COMPOSITIONS_AND_UPLOADS_ONLY = 'Y';
    } else if (filter === 'Your Repertoire') {
      baseParams.YN_YOUR_REPERTOIRE_ONLY = 'Y';
    } else if (filter === 'Songs Shared With You') {
      baseParams.YN_SONGS_SHARED_WITH_YOU_ONLY = 'Y';
    } else if (filter === 'Your Recordings') {
      baseParams.YN_YOUR_RECORDINGS_ONLY = 'Y';
    } else if (filter === 'Recordings Shared With You') {
      baseParams.YN_RECORDINGS_SHARED_WITH_YOU_ONLY = 'Y';
    }

    const cleanParams = Object.fromEntries(
      Object.entries(baseParams).filter(([_, v]) => v !== null)
    );

    try {
      const response = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ SP_NAME: spName, PARAMS: cleanParams }),
      });
      const json = await response.json();
      setResults(sortResults(json.RESULT || []));
    } catch (error) {
      console.error('Error fetching results:', error);
    }
  }

  const handleSort = (field) => {
    const newDirection = sortField === field && sortDirection === 'asc' ? 'desc' : 'asc';
    setSortField(field);
    setSortDirection(newDirection);
    setResults(sortResults(results));
  };

const handleSelect = (item) => {
  CLIENT_APP_VARIABLES.SONG_ID = item.SONG_ID || null;
  CLIENT_APP_VARIABLES.RECORDING_ID = item.RECORDING_ID || null;
  CLIENT_APP_VARIABLES.SONG_NAME = item.SONG_NAME || null;
  CLIENT_APP_VARIABLES.BPM = item.BPM || null;
  CLIENT_APP_VARIABLES.SHARE_LEVEL = item.SHARE_LEVEL || null;
  CLIENT_APP_VARIABLES.SHARE_WITH_VIOLINIST_ID = item.SHARE_WITH_VIOLINIST_ID || null;
  CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME = item.AUDIO_STREAM_FILE_NAME || null;

  CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE = 'Play'; // ensure correct mode
  CLIENT_APP_VARIABLES.BREAKDOWN_NAME = "OVERALL"; // reset to default)",


  DEBUG_CONSOLE_LOG();

  router.push('/SCREEN_MAIN');
};



  const updateShareLevel = async (item, newLevel) => {
    CLIENT_APP_VARIABLES.SHARE_LEVEL = newLevel;
    item.SHARE_LEVEL = newLevel;
    const option = shareLevelOptions.find(opt => opt.PARAMETER_VALUE === newLevel);
    item.SHARE_LEVEL_DISPLAY_NAME = option ? option.PARAMETER_DISPLAY_VALUE : newLevel;
    setResults([...results]);
    logAppVariables();

    if (newLevel === 'SPECIFIC VIOLINST') {
      CLIENT_APP_VARIABLES.YN_SEARCH_IN_NETWORK_ONLY = 'Y';
      setSelectedItemForShare(item);
      setViolinistModalVisible(true);
      await fetchViolinists();
    } else {
      await callUpdateSP(item, newLevel, null);
    }
  };

  const fetchViolinists = async () => {
    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_DD_VIOLINIST',
          PARAMS: {
            SEARCH_BY_VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
            YN_SEARCH_IN_NETWORK_ONLY: 'Y',
          },
        }),
      });
      const json = await res.json();
      setViolinistList(json.RESULT || []);
    } catch (err) {
      console.error('Error fetching violinists', err);
    }
  };

  const callUpdateSP = async (item, shareLevel, shareWithViolinistId) => {
    const SP_NAME = mode === 'Song' ? 'P_CLIENT_SONG_UPD' : 'P_CLIENT_SONG_RECORDING_UPD';
    const ID_FIELD = mode === 'Song' ? 'SONG_ID' : 'RECORDING_ID';
    const payload = {
      SP_NAME,
      PARAMS: {
        [ID_FIELD]: item[ID_FIELD],
        SHARE_LEVEL: shareLevel,
        SHARE_WITH_VIOLINIST_ID: shareWithViolinistId,
      },
    };
    console.log('Calling SP with payload:', JSON.stringify(payload, null, 2));
    try {
      const response = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const result = await response.json();
      console.log('SP call result:', result);
    } catch (error) {
      console.error('Error calling update SP:', error);
    }
    DEBUG_CONSOLE_LOG();

  };

  const renderResult = ({ item }) => (
    <Pressable onPress={() => handleSelect(item)} style={styles.resultItem}>
      <Text style={styles.resultLine1}>
        {item.SONG_NAME || ''} {mode === 'Recording' ? item.ARTIST_NAME || '' : ''}
      </Text>
      <Text style={styles.resultLine2}>
        {item.COMPOSER_NAME || ''} | {item.DIFFICULTY_LEVEL || item.BPM || ''} | {item.TOP_SCORER_USER_DISPLAY_NAME || item.SCORE || ''} | {item.SHARE_LEVEL_DISPLAY_NAME || item.SHARE_LEVEL || ''} | {item.DATE_FOR_DISPLAY || ''}
      </Text>
      {item.YN_CAN_EDIT_SHARE_LEVEL === 'Y' && (
        <Picker
          selectedValue={item.SHARE_LEVEL}
          onValueChange={(value) => updateShareLevel(item, value)}>
          {shareLevelOptions.map(opt => (
            <Picker.Item
              key={opt.PARAMETER_VALUE}
              label={opt.PARAMETER_DISPLAY_VALUE}
              value={opt.PARAMETER_VALUE}
            />
          ))}
        </Picker>
      )}
    </Pressable>
  );

  useEffect(() => {
    fetchShareLevels();
  }, []);

  const fetchShareLevels = async () => {
    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_DD_PARAMETER_VALUE',
          PARAMS: {
            VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
            PARAMETER_NAME: 'SHARE LEVEL',
          },
        }),
      });
      const json = await res.json();
      setShareLevelOptions(json.RESULT || []);
    } catch (err) {
      console.error('Error fetching share-level options', err);
    }
  };

  const filters = mode === 'Song'
    ? ['All', 'Your Compositions/Uploads', 'Your Repertoire', 'Songs Shared With You']
    : ['All', 'Your Compositions/Uploads', 'Your Recordings', 'Recordings Shared With You'];

  const columnHeaders = mode === 'Song'
    ? [
        { label: 'Song', field: 'SONG_NAME' },
        { label: 'Difficulty', field: 'DIFFICULTY_LEVEL' },
        { label: 'Hi Score', field: 'TOP_SCORER_USER_DISPLAY_NAME' },
        { label: 'Shared', field: 'SHARE_LEVEL_DISPLAY_NAME' },
      ]
    : [
        { label: 'Song', field: 'SONG_NAME' },
        { label: 'Artist', field: 'ARTIST_NAME' },
        { label: 'BPM', field: 'BPM' },
        { label: 'Score', field: 'SCORE' },
        { label: 'Date', field: 'DATE_FOR_SORTING' },
        { label: 'Shared', field: 'SHARE_LEVEL_DISPLAY_NAME' },
      ];

  return (
    <View style={styles.container}>
      <View style={[styles.radioGroup, { marginTop: 20 }]}>
        {['Song', 'Recording'].map((opt) => (
          <TouchableOpacity key={opt} style={styles.radioBtn} onPress={() => setMode(opt)}>
            <Text style={mode === opt ? styles.radioSelected : styles.radioUnselected}>O</Text>
            <Text style={styles.radioLabel}>{opt}</Text>
          </TouchableOpacity>
        ))}
      </View>

      <TextInput
        style={styles.searchBox}
        placeholder="Search song or composer"
        value={searchText}
        onChangeText={setSearchText}
      />

      <View style={styles.radioGroup}>
        {filters.map((opt) => (
          <TouchableOpacity key={opt} style={styles.radioBtn} onPress={() => setFilter(opt)}>
            <Text style={filter === opt ? styles.radioSelected : styles.radioUnselected}>O</Text>
            <Text style={styles.radioLabel}>{opt}</Text>
          </TouchableOpacity>
        ))}
      </View>

      <View style={styles.columnHeaders}>
        {columnHeaders.map(({ label, field }) => (
          <TouchableOpacity key={label} onPress={() => handleSort(field)} style={{ flex: 1 }}>
            <Text style={styles.colHeader}>{label}</Text>
          </TouchableOpacity>
        ))}
      </View>

      <FlatList
        data={results}
        keyExtractor={(item, index) => index.toString()}
        renderItem={renderResult}
      />

      <Modal visible={violinistModalVisible} animationType="slide">
        <View style={{ flex: 1, padding: 20 }}>
          <Text>Select a Violinist</Text>
          {violinistList.map((v) => (
            <TouchableOpacity
              key={v.VIOLINIST_ID}
              onPress={async () => {
                CLIENT_APP_VARIABLES.SHARE_WITH_VIOLINIST_ID = v.VIOLINIST_ID;
                selectedItemForShare.SHARE_LEVEL_DISPLAY_NAME = v.USER_DISPLAY_NAME;
                await callUpdateSP(selectedItemForShare, 'SPECIFIC VIOLINST', v.VIOLINIST_ID);
                setViolinistModalVisible(false);
              }}>
              <Text style={{ padding: 10 }}>{v.USER_DISPLAY_NAME}</Text>
            </TouchableOpacity>
          ))}
          <Button title="Cancel" onPress={() => setViolinistModalVisible(false)} />
        </View>
      </Modal>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 16, backgroundColor: '#fff' },
  radioGroup: { flexDirection: 'row', flexWrap: 'wrap', marginBottom: 10 },
  radioBtn: { flexDirection: 'row', alignItems: 'center', marginRight: 15, marginVertical: 4 },
  radioLabel: { marginLeft: 4 },
  radioSelected: { color: '#007AFF', fontWeight: 'bold' },
  radioUnselected: { color: '#999' },
  searchBox: {
    height: 40,
    borderColor: '#ccc',
    borderWidth: 1,
    borderRadius: 8,
    paddingHorizontal: 10,
    marginBottom: 10,
  },
  columnHeaders: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingVertical: 5,
    borderBottomWidth: 1,
    borderColor: '#ccc',
  },
  colHeader: { fontWeight: 'bold', fontSize: 12, textAlign: 'left' },
  resultItem: { marginVertical: 8 },
  resultLine1: { fontWeight: 'bold' },
  resultLine2: { fontSize: 12, color: '#555' },
});
