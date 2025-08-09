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

// Shared column specs so headers align with row cells 1:1
const SONG_COLS = { song: 0.46, difficulty: 0.18, hiScore: 0.18, shared: 0.18 };
const REC_COLS  = { song: 0.40, artist: 0.22, bpm: 0.12, score: 0.12, date: 0.14 };

export default function SCREEN_SONG_SEARCH() {
  const [mode, setMode] = useState('Song');

  // Short labels on screen; backend flags use ids
  const SONG_FILTERS = [
    { id: 'ALL',            label: 'All' },
    { id: 'YOURS_UPLOADS',  label: 'Yours' },       // Your Compositions/Uploads
    { id: 'REPERTOIRE',     label: 'Repertoire' },  // Your Repertoire
    { id: 'SONGS_SHARED',   label: 'Shared' },      // Songs Shared With You
  ];
  const REC_FILTERS = [
    { id: 'ALL',            label: 'All' },
    { id: 'YOURS_UPLOADS',  label: 'Yours' },       // Your Compositions/Uploads
    { id: 'YOUR_RECORDINGS',label: 'Your Recs' },   // Your Recordings
    { id: 'RECS_SHARED',    label: 'Shared' },      // Recordings Shared With You
  ];

  const [filterId, setFilterId] = useState('ALL');
  const [searchText, setSearchText] = useState('');
  const [results, setResults] = useState([]);
  const [sortField, setSortField] = useState(null);
  const [sortDirection, setSortDirection] = useState('asc');

  const [shareLevelOptions, setShareLevelOptions] = useState([]);
  const [shareLevelEditorVisible, setShareLevelEditorVisible] = useState(false);
  const [violinistModalVisible, setViolinistModalVisible] = useState(false);
  const [violinistList, setViolinistList] = useState([]);
  const [selectedItemForShare, setSelectedItemForShare] = useState(null);

  useEffect(() => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(fetchResults, 250);
    return () => clearTimeout(searchDebounce);
  }, [mode, filterId, searchText]);

  function sortResults(data) {
    if (!sortField) return data;
    const sorted = [...data].sort((a, b) => {
      const av = a[sortField] ?? '';
      const bv = b[sortField] ?? '';
      if (typeof av === 'number' && typeof bv === 'number') {
        return sortDirection === 'asc' ? av - bv : bv - av;
      }
      return sortDirection === 'asc'
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
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

    if (filterId === 'YOURS_UPLOADS') baseParams.YN_YOUR_COMPOSITIONS_AND_UPLOADS_ONLY = 'Y';
    if (filterId === 'REPERTOIRE')    baseParams.YN_YOUR_REPERTOIRE_ONLY = 'Y';
    if (filterId === 'SONGS_SHARED')  baseParams.YN_SONGS_SHARED_WITH_YOU_ONLY = 'Y';
    if (filterId === 'YOUR_RECORDINGS') baseParams.YN_YOUR_RECORDINGS_ONLY = 'Y';
    if (filterId === 'RECS_SHARED')     baseParams.YN_RECORDINGS_SHARED_WITH_YOU_ONLY = 'Y';

    const cleanParams = Object.fromEntries(Object.entries(baseParams).filter(([, v]) => v !== null));

    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ SP_NAME: spName, PARAMS: cleanParams }),
      });
      const json = await res.json();
      const list = (json.RESULT || []).filter(r => r && r.SONG_NAME);
      setResults(sortResults(list));
    } catch (e) {
      console.error('Error fetching results:', e);
    }
  }

  const handleSort = (field) => {
    const dir = sortField === field && sortDirection === 'asc' ? 'desc' : 'asc';
    setSortField(field);
    setSortDirection(dir);
    setResults(sortResults(results));
  };

  const handleSelect = (item) => {
    CLIENT_APP_VARIABLES.SONG_ID = item.SONG_ID ?? null;
    CLIENT_APP_VARIABLES.RECORDING_ID = item.RECORDING_ID ?? null;
    CLIENT_APP_VARIABLES.SONG_NAME = item.SONG_NAME ?? null;
    CLIENT_APP_VARIABLES.BPM = item.BPM ?? null;
    CLIENT_APP_VARIABLES.SHARE_LEVEL = item.SHARE_LEVEL ?? null;
    CLIENT_APP_VARIABLES.SHARE_WITH_VIOLINIST_ID = item.SHARE_WITH_VIOLINIST_ID ?? null;
    CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME = item.AUDIO_STREAM_FILE_NAME ?? null;
    CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE = 'Play';
    CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
    DEBUG_CONSOLE_LOG();
    router.push('/SCREEN_MAIN');
  };

  const openShareEditor = (item) => {
    if (item.YN_CAN_EDIT_SHARE_LEVEL === 'Y') {
      setSelectedItemForShare(item);
      setShareLevelEditorVisible(true);
    }
  };

  const applyShareLevel = async (value) => {
    if (!selectedItemForShare) return;
    const opt = shareLevelOptions.find(o => o.PARAMETER_VALUE === value);
    selectedItemForShare.SHARE_LEVEL = value;
    selectedItemForShare.SHARE_LEVEL_DISPLAY_NAME = opt ? opt.PARAMETER_DISPLAY_VALUE : value;
    setResults([...results]);
    setShareLevelEditorVisible(false);

    if (value === 'SPECIFIC VIOLINST') {
      CLIENT_APP_VARIABLES.YN_SEARCH_IN_NETWORK_ONLY = 'Y';
      await fetchViolinists();
      setViolinistModalVisible(true);
    } else {
      await callUpdateSP(selectedItemForShare, value, null);
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
    const payload = { SP_NAME, PARAMS: { [ID_FIELD]: item[ID_FIELD], SHARE_LEVEL: shareLevel, SHARE_WITH_VIOLINIST_ID: shareWithViolinistId } };
    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
      });
      await res.json();
    } catch (e) { console.error('Error calling update SP:', e); }
    DEBUG_CONSOLE_LOG();
  };

  // ---------- Header & Rows ----------
  const HeaderRow = () => {
    const H = ({ label, onPress, flex }) => (
      <TouchableOpacity onPress={onPress} style={[styles.cell, { flex }]}>
        <Text style={styles.headerText}>{label}</Text>
      </TouchableOpacity>
    );

    return (
      <View style={styles.headerRow}>
        <H label="Song" onPress={() => handleSort('SONG_NAME')} flex={mode === 'Song' ? SONG_COLS.song : REC_COLS.song} />
        {mode === 'Song' ? (
          <>
            <H label="Difficulty" onPress={() => handleSort('DIFFICULTY_LEVEL')} flex={SONG_COLS.difficulty} />
            <H label="Hi Score"   onPress={() => handleSort('TOP_SCORER_USER_DISPLAY_NAME')} flex={SONG_COLS.hiScore} />
            <H label="Shared"     onPress={() => handleSort('SHARE_LEVEL_DISPLAY_NAME')} flex={SONG_COLS.shared} />
          </>
        ) : (
          <>
            <H label="Artist" onPress={() => handleSort('ARTIST_NAME')} flex={REC_COLS.artist} />
            <H label="BPM"    onPress={() => handleSort('BPM')}         flex={REC_COLS.bpm} />
            <H label="Score"  onPress={() => handleSort('SCORE')}       flex={REC_COLS.score} />
            <H label="Date"   onPress={() => handleSort('DATE_FOR_SORTING')} flex={REC_COLS.date} />
          </>
        )}
      </View>
    );
  };

  const SongRow = ({ item, index }) => {
    const bg = index % 2 === 0 ? '#FFFFFF' : '#F7F7F7';
    const songName = item.SONG_NAME ?? '';
    const composer = item.COMPOSER_NAME ?? item.USER_DISPLAY_NAME ?? '';
    const difficulty = item.DIFFICULTY_LEVEL ?? item.DIFFICULTY ?? '';
    const hiScore = item.TOP_SCORER_USER_DISPLAY_NAME ?? item.TOP_SCORE ?? 0;
    const sharedDisplay = item.SHARE_LEVEL_DISPLAY_NAME ?? '';

    return (
      <Pressable onPress={() => handleSelect(item)} style={[styles.rowWrap, { backgroundColor: bg }]}>
        {/* Line 1: Song title */}
        <Text style={styles.songText} numberOfLines={1} adjustsFontSizeToFit minimumFontScale={0.82}>
          {songName}
        </Text>

        {/* Line 2: columns aligned with header */}
        <View style={styles.line2}>
          <View style={{ flex: SONG_COLS.song }}><Text style={styles.subText} numberOfLines={1}>{composer}</Text></View>
          <View style={{ flex: SONG_COLS.difficulty }}><Text style={styles.subText}>{difficulty}</Text></View>
          <View style={{ flex: SONG_COLS.hiScore }}><Text style={styles.subText}>{hiScore || 0}</Text></View>
          <TouchableOpacity
            activeOpacity={item.YN_CAN_EDIT_SHARE_LEVEL === 'Y' ? 0.5 : 1}
            onPress={() => openShareEditor(item)}
            style={{ flex: SONG_COLS.shared }}
          >
            <Text style={[styles.subText, item.YN_CAN_EDIT_SHARE_LEVEL === 'Y' && styles.linkText]} numberOfLines={1}>
              {sharedDisplay}
            </Text>
          </TouchableOpacity>
        </View>
      </Pressable>
    );
  };

  const RecordingRow = ({ item, index }) => {
    const bg = index % 2 === 0 ? '#FFFFFF' : '#F7F7F7';
    const oneLineProps = { numberOfLines: 1, ellipsizeMode: 'tail', adjustsFontSizeToFit: true, minimumFontScale: 0.82 };

    return (
      <Pressable onPress={() => handleSelect(item)} style={[styles.rowWrap, { backgroundColor: bg }]}>
        {/* Line 1: Song title */}
        <Text style={styles.songText} {...oneLineProps}>{item.SONG_NAME ?? ''}</Text>

        {/* Line 2: Song→COMPOSER_NAME, then Artist/BPM/Score/Date */}
        <View style={styles.line2}>
          <View style={{ flex: REC_COLS.song }}>
            <Text style={styles.subText} {...oneLineProps}>{item.COMPOSER_NAME ?? ''}</Text>
          </View>
          <View style={{ flex: REC_COLS.artist }}>
            <Text style={styles.subText} {...oneLineProps}>{item.ARTIST_NAME ?? ''}</Text>
          </View>
          <View style={{ flex: REC_COLS.bpm }}>
            <Text style={styles.subText} {...oneLineProps}>{item.BPM ?? ''}</Text>
          </View>
          <View style={{ flex: REC_COLS.score }}>
            <Text style={styles.subText} {...oneLineProps}>{item.SCORE ?? 0}</Text>
          </View>
          <View style={{ flex: REC_COLS.date }}>
            <Text style={styles.subText} {...oneLineProps}>{item.DATE_FOR_DISPLAY ?? ''}</Text>
          </View>
        </View>
      </Pressable>
    );
  };

  useEffect(() => { fetchShareLevels(); }, []);
  const fetchShareLevels = async () => {
    try {
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_DD_PARAMETER_VALUE',
          PARAMS: { VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID, PARAMETER_NAME: 'SHARE LEVEL' },
        }),
      });
      const json = await res.json();
      setShareLevelOptions(json.RESULT || []);
    } catch (err) { console.error('Error fetching share-level options', err); }
  };

  const filters = mode === 'Song' ? SONG_FILTERS : REC_FILTERS;

  return (
    <View style={styles.container}>
      {/* Mode toggle */}
      <View style={[styles.radioGroup, { marginTop: 12 }]}>
        {['Song','Recording'].map(opt => (
          <TouchableOpacity key={opt} style={styles.radioBtn} onPress={() => { setMode(opt); setFilterId('ALL'); }}>
            <Text style={mode === opt ? styles.radioSelected : styles.radioUnselected}>O</Text>
            <Text style={styles.radioLabel}>{opt}</Text>
          </TouchableOpacity>
        ))}
      </View>

      {/* Search */}
      <TextInput
        style={styles.searchBox}
        placeholder="Search song or composer"
        value={searchText}
        onChangeText={setSearchText}
      />

      {/* Filters (short labels) */}
      <View style={styles.radioGroup}>
        {filters.map(opt => (
          <TouchableOpacity key={opt.id} style={styles.radioBtn} onPress={() => setFilterId(opt.id)}>
            <Text style={filterId === opt.id ? styles.radioSelected : styles.radioUnselected}>O</Text>
            <Text style={styles.radioLabel}>{opt.label}</Text>
          </TouchableOpacity>
        ))}
      </View>

      <HeaderRow />

      <FlatList
        data={results}
        keyExtractor={(item, index) => (item?.SONG_ID ?? item?.RECORDING_ID ?? index).toString()}
        renderItem={({ item, index }) => {
          if (!item || !item.SONG_NAME) return null;
          return mode === 'Song' ? <SongRow item={item} index={index} /> : <RecordingRow item={item} index={index} />;
        }}
        contentContainerStyle={{ paddingBottom: 0 }}
        removeClippedSubviews
        initialNumToRender={20}
        windowSize={7}
      />

      {/* Share-level editor modal */}
      <Modal
        visible={shareLevelEditorVisible}
        transparent
        animationType="fade"
        onRequestClose={() => setShareLevelEditorVisible(false)}
      >
        <View style={styles.modalBackdrop}>
          <View style={styles.modalCard}>
            <Text style={styles.modalTitle}>Change Share Level</Text>
            <Picker
              selectedValue={selectedItemForShare?.SHARE_LEVEL}
              onValueChange={applyShareLevel}
            >
              {shareLevelOptions.map(opt => (
                <Picker.Item key={opt.PARAMETER_VALUE} label={opt.PARAMETER_DISPLAY_VALUE} value={opt.PARAMETER_VALUE} />
              ))}
            </Picker>
            <Button title="Cancel" onPress={() => setShareLevelEditorVisible(false)} />
          </View>
        </View>
      </Modal>

      {/* Violinist selector for SPECIFIC VIOLINST */}
      <Modal visible={violinistModalVisible} animationType="slide" onRequestClose={() => setViolinistModalVisible(false)}>
        <View style={{ flex: 1, padding: 16 }}>
          <Text>Select a Violinist</Text>
          {violinistList.map(v => (
            <TouchableOpacity
              key={v.VIOLINIST_ID}
              onPress={async () => {
                CLIENT_APP_VARIABLES.SHARE_WITH_VIOLINIST_ID = v.VIOLINIST_ID;
                selectedItemForShare.SHARE_LEVEL_DISPLAY_NAME = v.USER_DISPLAY_NAME; // display only
                await callUpdateSP(selectedItemForShare, 'SPECIFIC VIOLINST', v.VIOLINIST_ID);
                setViolinistModalVisible(false);
              }}
            >
              <Text style={{ paddingVertical: 8 }}>{v.USER_DISPLAY_NAME}</Text>
            </TouchableOpacity>
          ))}
          <Button title="Cancel" onPress={() => setViolinistModalVisible(false)} />
        </View>
      </Modal>
    </View>
  );
}

const PAD_H = 8;

const styles = StyleSheet.create({
  container: { flex: 1, paddingHorizontal: 12, paddingTop: 8, backgroundColor: '#fff' },

  radioGroup: { flexDirection: 'row', flexWrap: 'wrap', marginBottom: 6 },
  radioBtn: { flexDirection: 'row', alignItems: 'center', marginRight: 12, marginVertical: 2 },
  radioLabel: { marginLeft: 4, fontSize: 13 },
  radioSelected: { color: '#007AFF', fontWeight: '600' },
  radioUnselected: { color: '#999' },

  searchBox: {
    height: 36, borderColor: '#ccc', borderWidth: 1, borderRadius: 8,
    paddingHorizontal: 10, marginBottom: 6, fontSize: 14,
  },

  headerRow: {
    flexDirection: 'row', alignItems: 'center', backgroundColor: '#F2F2F7',
    paddingVertical: 6, paddingHorizontal: PAD_H,
    borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#D1D1D6',
  },
  headerText: { fontWeight: '700', fontSize: 12, color: '#111' },

  // Rows — same horizontal padding as header so columns line up precisely
  rowWrap: { paddingVertical: 6, paddingHorizontal: PAD_H },
  songText: { fontSize: 15, fontWeight: '700', color: '#111' },

  line2: { flexDirection: 'row', alignItems: 'center', marginTop: 2 },
  cell: { justifyContent: 'center', paddingRight: PAD_H }, // header cells
  subText: { fontSize: 12.5, color: '#333' },
  linkText: { textDecorationLine: 'underline' },

  modalBackdrop: { flex: 1, backgroundColor: 'rgba(0,0,0,0.25)', justifyContent: 'center', alignItems: 'center' },
  modalCard: { width: '86%', backgroundColor: '#fff', borderRadius: 10, padding: 12 },
  modalTitle: { fontWeight: '700', marginBottom: 8 },
});
