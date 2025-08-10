// SCREEN_NETWORKING.js
import { useFocusEffect } from '@react-navigation/native';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
    ActivityIndicator,
    Modal,
    Pressable,
    StyleSheet,
    Switch,
    Text,
    TextInput,
    TouchableOpacity,
    View,
} from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

// ---------- tiny renderer kicker (no UI state stored here) ----------
function useForceRender() {
  const [, setTick] = useState(0);
  return () => setTick((t) => (t + 1) % 10000);
}

// ---------- helpers ----------
function abbrSource(s) {
  if (!s) return '';
  const t = String(s).toLowerCase();
  if (t.startsWith('violinist')) return 'Search';
  if (t.startsWith('automatic')) return 'Auto';
  if (t.startsWith('top')) return 'Top';
  if (t.startsWith('composition')) return 'Comp';
  if (t === 'you') return 'You';
  return s;
}
function sortByField(list, field, dir) {
  const a = [...list];
  a.sort((x, y) => {
    const xv = x?.[field];
    const yv = y?.[field];
    if (typeof xv === 'number' && typeof yv === 'number') {
      return dir === 'asc' ? xv - yv : yv - xv;
    }
    return dir === 'asc'
      ? String(xv ?? '').localeCompare(String(yv ?? ''))
      : String(yv ?? '').localeCompare(String(xv ?? ''));
  });
  return a;
}

// ================================================================
//                           SCREEN
// ================================================================
export default function SCREEN_NETWORKING() {
  const forceRender = useForceRender();

  // refs for "procedural" data
  const networkRowsRef = useRef([]);            // table rows (from P_CLIENT_NETWORK_GET)
  const loadingRef = useRef(false);

  const searchVisibleRef = useRef(false);
  const searchResultsRef = useRef([]);          // modal results (from P_CLIENT_DD_VIOLINIST)
  const searchTextRef = useRef('');
  const searchDebounceRef = useRef(null);

  const sortRef = useRef({ field: 'NETWORK_MEMBER_VIOLINIST_DISPLAY_NAME', dir: 'asc' });

  // store current share mode per id (Both/You/Them) so UI is stable before round-trip
  if (!CLIENT_APP_VARIABLES.SHARE_MODE_BY_ID) CLIENT_APP_VARIABLES.SHARE_MODE_BY_ID = {};

  // ---------------------------------------------------------------
  // CALLERS
  // ---------------------------------------------------------------
  async function callSP(spName, params) {
    try {
      console.log('[SP] →', spName, params);
      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ SP_NAME: spName, PARAMS: params }),
      });
      const json = await res.json();
      console.log('[SP] √', spName, 'rows:', json.RESULT?.length ?? 'n/a');
      return json.RESULT || [];
    } catch (err) {
      console.error('[SP] ✗', spName, err);
      return [];
    }
  }

  async function fetchNetwork() {
    console.log('Start function fetchNetwork');
    loadingRef.current = true;
    forceRender();

    const rows = await callSP('P_CLIENT_NETWORK_GET', {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
    });

    // seed share mode by row flags
    rows.forEach((r) => {
      const id = r.NETWORK_MEMBER_VIOLINIST_ID;
      let mode = 'both';
      if (r.YN_SHARE_YOUR_MUSIC_TO_MEMBER === 'Y' && r.YN_SHARE_MEMBERS_MUSIC_TO_YOU !== 'Y') mode = 'your';
      if (r.YN_SHARE_YOUR_MUSIC_TO_MEMBER !== 'Y' && r.YN_SHARE_MEMBERS_MUSIC_TO_YOU === 'Y') mode = 'member';
      CLIENT_APP_VARIABLES.SHARE_MODE_BY_ID[id] = mode;
    });

    // apply current sort
    const { field, dir } = sortRef.current;
    networkRowsRef.current = sortByField(rows, field, dir);

    loadingRef.current = false;
    forceRender();
  }

  async function runSearch() {
    console.log('Start function runSearch');
    const q = searchTextRef.current?.trim();
    if (!q) {
      searchResultsRef.current = [];
      searchVisibleRef.current = false;
      forceRender();
      return;
    }
    const results = await callSP('P_CLIENT_DD_VIOLINIST', {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      SEARCH_FOR_FILTER_TEXT: q,
    });
    searchResultsRef.current = results;
    searchVisibleRef.current = true;
    forceRender();
  }

  function changeSort(field) {
    const s = sortRef.current;
    const dir = s.field === field && s.dir === 'asc' ? 'desc' : 'asc';
    sortRef.current = { field, dir };
    networkRowsRef.current = sortByField(networkRowsRef.current, field, dir);
    forceRender();
  }

  // Share segmented control → update app-vars then send SP (ACTION null)
  async function applyShareMode(violinistId, mode) {
    console.log('Start function applyShareMode', { violinistId, mode });
    CLIENT_APP_VARIABLES.SHARE_MODE_BY_ID[violinistId] = mode;

    const ynYou = mode === 'both' || mode === 'your' ? 'Y' : null;
    const ynThem = mode === 'both' || mode === 'member' ? 'Y' : null;

    await callSP('P_CLIENT_NETWORK_INS', {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      NETWORK_MEMBER_VIOLINIST_ID: violinistId,
      ACTION: null,
      INVITATION_SOURCE: null,
      VIOLINIST_SEARCH_LOG_ID: null,
      YN_SHARE_YOUR_MUSIC_TO_MEMBER: ynYou,
      YN_SHARE_MEMBERS_MUSIC_TO_YOU: ynThem,
    });

    // reflect locally for a snappy UI
    const found = networkRowsRef.current.find(r => r.NETWORK_MEMBER_VIOLINIST_ID === violinistId);
    if (found) {
      found.YN_SHARE_YOUR_MUSIC_TO_MEMBER = ynYou;
      found.YN_SHARE_MEMBERS_MUSIC_TO_YOU = ynThem;
    }
    forceRender();
  }

  // Action buttons (Accept / Reject / Rescind / Remove)
  async function doAction(violinistId, action) {
    console.log('Start function doAction', { violinistId, action });
    await callSP('P_CLIENT_NETWORK_INS', {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      NETWORK_MEMBER_VIOLINIST_ID: violinistId,
      ACTION: action,
      INVITATION_SOURCE: null,
      VIOLINIST_SEARCH_LOG_ID: null,
      YN_SHARE_YOUR_MUSIC_TO_MEMBER: null,
      YN_SHARE_MEMBERS_MUSIC_TO_YOU: null,
    });
    await fetchNetwork();
  }

  // Search modal → Send Invite button
  async function sendInvite(row) {
    console.log('Start function sendInvite');
    CLIENT_APP_VARIABLES.NETWORK_MEMBER_VIOLINIST_ID = row.VIOLINIST_ID;
    CLIENT_APP_VARIABLES.VIOLINIST_SEARCH_LOG_ID = row.VIOLINIST_SEARCH_LOG_ID;

    await callSP('P_CLIENT_NETWORK_INS', {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      NETWORK_MEMBER_VIOLINIST_ID: row.VIOLINIST_ID,
      ACTION: 'Send Invite',
      INVITATION_SOURCE: 'VIOLINIST SEARCH',
      VIOLINIST_SEARCH_LOG_ID: row.VIOLINIST_SEARCH_LOG_ID,
      YN_SHARE_YOUR_MUSIC_TO_MEMBER: null,
      YN_SHARE_MEMBERS_MUSIC_TO_YOU: null,
    });

    searchVisibleRef.current = false;
    forceRender();
    await fetchNetwork();
  }

  // Pref toggles (persist with your existing SP that updates violinist prefs)
  async function updatePref(field, value) {
    console.log('Start function updatePref', field, value);
    CLIENT_APP_VARIABLES[field] = value ? 'Y' : null;
    // If you have a SP for this, call it here. Example:
    await callSP('P_CLIENT_VIOLINIST_UPD', {
      VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
      [field]: value ? 'Y' : null,
    });
    forceRender();
  }

  // ---------------------------------------------------------------
  // LIFECYCLE
  // ---------------------------------------------------------------
  useEffect(() => {
    // initial
    fetchNetwork();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useFocusEffect(
    useCallback(() => {
      // refresh on focus
      fetchNetwork();
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [])
  );

  // ---------------------------------------------------------------
  // RENDER
  // ---------------------------------------------------------------
  const rows = networkRowsRef.current;
  const loading = loadingRef.current;
  const sort = sortRef.current;
  const searchVisible = searchVisibleRef.current;
  const searchResults = searchResultsRef.current;

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Connect with Musicians</Text>

      {/* toggles */}
      <RowSwitch
        label="Hide my Location"
        value={CLIENT_APP_VARIABLES.YN_NETWORKING_HIDE_MY_LOCATION === 'Y'}
        onValueChange={(v) => updatePref('YN_NETWORKING_HIDE_MY_LOCATION', v)}
      />
      <RowSwitch
        label="Hide me from violinist search"
        value={CLIENT_APP_VARIABLES.YN_NETWORKING_HIDE_ME_FROM_SEARCH === 'Y'}
        onValueChange={(v) => updatePref('YN_NETWORKING_HIDE_ME_FROM_SEARCH', v)}
      />
      <RowSwitch
        label="Receive Automatic Introductions"
        value={CLIENT_APP_VARIABLES.YN_NETWORKING_HIDE_ME_FROM_AUTOMATIC_INTRODUCTIONS !== 'Y'}
        onValueChange={(v) =>
          updatePref('YN_NETWORKING_HIDE_ME_FROM_AUTOMATIC_INTRODUCTIONS', !v ? 'Y' : null) // invert semantics
        }
        invertSemantics
      />

      {/* search */}
      <View style={styles.searchRow}>
        <View style={styles.searchWrap}>
          <Text style={styles.searchIcon}>🔍</Text>
          <TextInput
            style={styles.searchInput}
            placeholder="Search for Violinist"
            onChangeText={(text) => {
              searchTextRef.current = text;
              if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
              searchDebounceRef.current = setTimeout(runSearch, 300);
            }}
            returnKeyType="search"
            onSubmitEditing={runSearch}
          />
        </View>
        <TouchableOpacity style={styles.refreshBtn} onPress={fetchNetwork} activeOpacity={0.8}>
          <Text style={styles.refreshText}>Refresh</Text>
        </TouchableOpacity>
      </View>

      {/* table */}
      <View style={styles.table}>
        <View style={styles.thead}>
          <HeaderCell
            label="Violinist"
            flex={0.40}
            active={sort.field === 'NETWORK_MEMBER_VIOLINIST_DISPLAY_NAME'}
            dir={sort.dir}
            onPress={() => changeSort('NETWORK_MEMBER_VIOLINIST_DISPLAY_NAME')}
          />
          <HeaderCell
            label="Level"
            flex={0.20}
            active={sort.field === 'NETWORK_MEMBER_VIOLINIST_DISPLAY_LEVEL'}
            dir={sort.dir}
            onPress={() => changeSort('NETWORK_MEMBER_VIOLINIST_DISPLAY_LEVEL')}
          />
          <HeaderCell
            label="Miles Away"
            flex={0.20}
            active={sort.field === 'NETWORK_MEMBER_MILES_FROM_YOU'}
            dir={sort.dir}
            onPress={() => changeSort('NETWORK_MEMBER_MILES_FROM_YOU')}
          />
          <HeaderCell
            label="Source"
            flex={0.20}
            active={sort.field === 'INVITATION_SOURCE'}
            dir={sort.dir}
            onPress={() => changeSort('INVITATION_SOURCE')}
          />
        </View>

        {loading ? (
          <View style={{ padding: 16, alignItems: 'center' }}>
            <ActivityIndicator />
          </View>
        ) : rows.length === 0 ? (
          <Text style={{ padding: 12, color: '#666' }}>No connections yet.</Text>
        ) : (
          rows.map((r, idx) => (
            <View key={`${r.NETWORK_MEMBER_VIOLINIST_ID}-${idx}`} style={styles.row2line}>
              {/* LINE 1 */}
              <View style={styles.rowTop}>
                <Text style={[styles.tcell, { flex: 0.40 }]} numberOfLines={1} ellipsizeMode="tail">
                  {r.NETWORK_MEMBER_VIOLINIST_DISPLAY_NAME}
                </Text>
                <Text style={[styles.tcell, { flex: 0.20 }]} numberOfLines={1} ellipsizeMode="tail">
                  {r.NETWORK_MEMBER_VIOLINIST_DISPLAY_LEVEL}
                </Text>
                <Text style={[styles.tcell, { flex: 0.20, textAlign: 'right', paddingRight: 6 }]}>
                  {r.NETWORK_MEMBER_MILES_FROM_YOU ?? ''}
                </Text>
                <Text style={[styles.tcell, { flex: 0.20 }]} numberOfLines={1} ellipsizeMode="tail">
                  {abbrSource(r.INVITATION_SOURCE)}
                </Text>
              </View>

              {/* LINE 2 */}
              <View style={styles.rowBottom}>
                <View style={styles.shareLeft}>
                  <Text style={styles.shareLabel}>Share: </Text>
                  <View style={styles.segmentWrap}>
                    {['both', 'your', 'member'].map((mode) => {
                      const selected =
                        CLIENT_APP_VARIABLES.SHARE_MODE_BY_ID[r.NETWORK_MEMBER_VIOLINIST_ID] === mode;
                      const label = mode === 'both' ? 'Both' : mode === 'your' ? 'You' : 'Them';
                      return (
                        <TouchableOpacity
                          key={mode}
                          onPress={() => applyShareMode(r.NETWORK_MEMBER_VIOLINIST_ID, mode)}
                          style={[styles.segmentPill, selected && styles.segmentPillSelected]}
                        >
                          <Text
                            style={[styles.segmentText, selected && styles.segmentTextSelected]}
                            numberOfLines={1}
                          >
                            {label}
                          </Text>
                        </TouchableOpacity>
                      );
                    })}
                  </View>
                </View>

                <View style={styles.actionsRight}>
                  {(r.ACTION_CHOICES || '')
                    .split(',')
                    .map((a) => a.trim())
                    .filter(Boolean)
                    .map((act) => (
                      <TouchableOpacity
                        key={act}
                        onPress={() => doAction(r.NETWORK_MEMBER_VIOLINIST_ID, act)}
                        style={styles.actionBtnSm}
                        activeOpacity={0.85}
                      >
                        <Text style={styles.actionBtnSmText} numberOfLines={1}>
                          {act}
                        </Text>
                      </TouchableOpacity>
                    ))}
                </View>
              </View>
            </View>
          ))
        )}
      </View>

      {/* SEARCH MODAL */}
      <Modal transparent visible={!!searchVisible} animationType="fade" onRequestClose={() => {
        searchVisibleRef.current = false; forceRender();
      }}>
        <Pressable style={styles.modalBackdrop} onPress={() => { searchVisibleRef.current = false; forceRender(); }} />
        <View style={styles.modal}>
          <Text style={styles.modalTitle}>Search Results</Text>
          {searchResults.length === 0 ? (
            <Text style={{ color: '#666' }}>No results.</Text>
          ) : (
            searchResults.map((row, i) => (
              <View key={`${row.VIOLINIST_ID}-${i}`} style={styles.modalRow}>
                <View style={{ flex: 0.44 }}>
                  <Text style={styles.modalName} numberOfLines={1}>{row.USER_DISPLAY_NAME}</Text>
                </View>
                <Text style={{ flex: 0.22 }} numberOfLines={1}>{row.VIOLINIST_LEVEL_DISPLAY_NAME}</Text>
                <Text style={{ flex: 0.18, textAlign: 'right' }}>{row.MILES_FROM_YOU ?? ''}</Text>
                <View style={{ flex: 0.16, alignItems: 'flex-end' }}>
                  {row.YN_SHOW_SEND_INVITE_COMMAND_BUTTON === 'Y' ? (
                    <TouchableOpacity style={styles.inviteBtn} onPress={() => sendInvite(row)}>
                      <Text style={styles.inviteBtnText}>Send Invite</Text>
                    </TouchableOpacity>
                  ) : null}
                </View>
              </View>
            ))
          )}
        </View>
      </Modal>
    </View>
  );
}

// ---------- small presentational bits ----------
function RowSwitch({ label, value, onValueChange, invertSemantics }) {
  return (
    <View style={styles.settingRow}>
      <Text style={styles.settingLabel}>{label}</Text>
      <Switch
        value={!!value}
        onValueChange={(v) => onValueChange(invertSemantics ? !v : v)}
        thumbColor="#fff"
        trackColor={{ false: '#d1d1d6', true: '#34C759' }}
      />
    </View>
  );
}

function HeaderCell({ label, onPress, active, dir, flex }) {
  return (
    <TouchableOpacity onPress={onPress} style={[styles.thcell, { flex }]} activeOpacity={0.6}>
      <Text style={styles.thtext} numberOfLines={1} ellipsizeMode="tail">
        {label}{active ? (dir === 'asc' ? ' ▲' : ' ▼') : ''}
      </Text>
    </TouchableOpacity>
  );
}

// ---------------------------------------------------------------
// STYLES
// ---------------------------------------------------------------
const styles = StyleSheet.create({
  container: { flex: 1, padding: 16, backgroundColor: '#fff' },

  title: { fontSize: 22, fontWeight: '800', marginBottom: 10, color: '#111' },

  settingRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginVertical: 6,
  },
  settingLabel: { fontSize: 16, color: '#111' },

  searchRow: { flexDirection: 'row', alignItems: 'center', gap: 8, marginTop: 8, marginBottom: 8 },
  searchWrap: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    borderWidth: 1, borderColor: '#ccc', borderRadius: 10,
    paddingHorizontal: 10, height: 38,
  },
  searchIcon: { marginRight: 6, color: '#666' },
  searchInput: { flex: 1 },

  refreshBtn: { backgroundColor: '#111', paddingHorizontal: 12, height: 36, borderRadius: 10, justifyContent: 'center' },
  refreshText: { color: '#fff', fontWeight: '700' },

  table: { borderWidth: 1, borderColor: '#e6e6ea', borderRadius: 12, overflow: 'hidden' },
  thead: { flexDirection: 'row', backgroundColor: '#f2f2f7', paddingVertical: 8, paddingHorizontal: 10 },
  thcell: { paddingRight: 8 },
  thtext: { fontSize: 13, fontWeight: '700', color: '#111' },

  row2line: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: '#eee',
    paddingHorizontal: 10,
    paddingVertical: 6,
    backgroundColor: '#fff',
  },
  rowTop: { flexDirection: 'row', alignItems: 'center', minHeight: 24, marginBottom: 4 },
  rowBottom: { flexDirection: 'row', alignItems: 'center', minHeight: 28 },

  tcell: { fontSize: 14, color: '#111', paddingRight: 8 },

  shareLeft: { flexDirection: 'row', alignItems: 'center', flex: 1 },
  shareLabel: { fontSize: 13, color: '#333', marginRight: 6 },

  segmentWrap: { flexDirection: 'row', gap: 6 },
  segmentPill: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: '#ccc',
    backgroundColor: '#fff',
  },
  segmentPillSelected: { backgroundColor: '#111', borderColor: '#111' },
  segmentText: { fontSize: 12, color: '#111' },
  segmentTextSelected: { color: '#fff', fontWeight: '700' },

  actionsRight: { flexDirection: 'row', gap: 8, justifyContent: 'flex-end' },

  actionBtnSm: { paddingHorizontal: 10, paddingVertical: 6, borderRadius: 10, backgroundColor: '#111' },
  actionBtnSmText: { color: '#fff', fontSize: 12, fontWeight: '700' },

  // modal
  modalBackdrop: {
    position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
    backgroundColor: 'rgba(0,0,0,0.25)',
  },
  modal: {
    position: 'absolute',
    top: '18%',
    left: '5%',
    right: '5%',
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 14,
    maxHeight: '64%',
  },
  modalTitle: { fontSize: 16, fontWeight: '800', marginBottom: 8 },
  modalRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: '#eee',
  },
  modalName: { fontSize: 14, fontWeight: '700', color: '#111' },
  inviteBtn: { backgroundColor: '#111', paddingHorizontal: 10, paddingVertical: 6, borderRadius: 8 },
  inviteBtnText: { color: '#fff', fontSize: 12, fontWeight: '700' },
});
