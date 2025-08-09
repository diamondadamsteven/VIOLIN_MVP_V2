// SCREEN_MAIN_6_COMMAND_BUTTONS.js
import { forwardRef, useImperativeHandle, useState } from 'react';
import { ScrollView, StyleSheet, Text, TouchableOpacity, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

const TAG = '🟦 SCREEN_MAIN_6_COMMAND_BUTTONS';

import { CB_COMPOSE_DISCARD } from './CB_COMPOSE_DISCARD';
import { CB_MUSIC_NOTES_EXPORT_TO_MUSICXML } from './CB_MUSIC_NOTES_EXPORT_TO_MUSICXML';
import { CB_MUSIC_NOTES_SHOW_ADVANCED } from './CB_MUSIC_NOTES_SHOW_ADVANCED';
import { CB_PRACTICE_GO_TO_NEXT_SECTION } from './CB_PRACTICE_GO_TO_NEXT_SECTION';
import { CB_PRACTICE_MODE_EXIT } from './CB_PRACTICE_MODE_EXIT';
import { CB_PRACTICE_MODE_OPEN } from './CB_PRACTICE_MODE_OPEN';

const COMMAND_REGISTRY = {
  CB_PRACTICE_MODE_EXIT,
  CB_PRACTICE_GO_TO_NEXT_SECTION,
  CB_COMPOSE_DISCARD,
  CB_MUSIC_NOTES_EXPORT_TO_MUSICXML,
  CB_PRACTICE_MODE_OPEN,
  CB_MUSIC_NOTES_SHOW_ADVANCED,
};

const SCREEN_MAIN_6_COMMAND_BUTTONS = forwardRef((props, ref) => {
  const [RESULT_SET_P_CLIENT_DD_COMMAND_BUTTONS, SET_RESULT_SET_P_CLIENT_DD_COMMAND_BUTTONS] = useState([]);

  const REFRESH = async () => {
    try {
      const URL = `${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`;
      const BODY = {
        SP_NAME: 'P_CLIENT_DD_COMMAND_BUTTONS',
        PARAMS: {
          VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID ?? -1,
          COMPOSE_PLAY_OR_PRACTICE: CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE ?? 'Play',
        },
      };

      console.log(`${TAG} → Fetching buttons from: ${URL}`);
      console.log(`${TAG} → Request body:`, BODY);

      const res = await fetch(URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(BODY),
      });

      const json = await res.json();
      const rows = Array.isArray(json?.RESULT) ? json.RESULT : [];
      rows.sort((a, b) => (a?.DISPLAY_ORDER_NO ?? 0) - (b?.DISPLAY_ORDER_NO ?? 0));

      console.log(`${TAG} → Loaded ${rows.length} buttons`);
      SET_RESULT_SET_P_CLIENT_DD_COMMAND_BUTTONS(rows);
    } catch (err) {
      console.error(`${TAG} ❌ REFRESH failed:`, err);
    }
  };

  useImperativeHandle(ref, () => ({ REFRESH }), []);

  const USER_TAPPED_COMMAND_BUTTON = (btn) => {
    const name = btn?.COMMAND_BUTTON_NAME;
    const fn = name ? COMMAND_REGISTRY[name] : null;

    if (typeof fn === 'function') {
      console.log(`${TAG} → Executing ${name}()`);
      try {
        fn();
      } catch (err) {
        console.error(`${TAG} ❌ Command "${name}" threw:`, err);
      }
    } else {
      console.warn(`${TAG} ⚠ No handler for COMMAND_BUTTON_NAME="${name}". Add it to COMMAND_REGISTRY.`);
    }
  };

  return (
    <View style={styles.wrapper}>
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={[styles.row, styles.centerContent]}
      >
        {RESULT_SET_P_CLIENT_DD_COMMAND_BUTTONS.map((btn, idx) => (
          <TouchableOpacity
            key={`${btn?.COMMAND_BUTTON_NAME ?? 'btn'}-${idx}`}
            style={styles.pill}
            onPress={() => USER_TAPPED_COMMAND_BUTTON(btn)}
          >
            <Text style={styles.pillLabel}>
              {btn?.COMMAND_BUTTON_DISPLAY_NAME ?? btn?.COMMAND_BUTTON_NAME ?? 'Button'}
            </Text>
          </TouchableOpacity>
        ))}

        {RESULT_SET_P_CLIENT_DD_COMMAND_BUTTONS.length === 0 && (
          <Text style={styles.emptyText}>No commands</Text>
        )}
      </ScrollView>
    </View>
  );
});

export default SCREEN_MAIN_6_COMMAND_BUTTONS;

const styles = StyleSheet.create({
  wrapper: {
    width: '100%',          // ensure full width so centering works
    alignItems: 'center',   // center the ScrollView if it sizes to content
    paddingHorizontal: 12,
    paddingTop: 4,
    paddingBottom: 0,
  },
  row: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingVertical: 4,
  },
  centerContent: {
    justifyContent: 'center', // <-- center the pills horizontally
    alignItems: 'center',
  },
  pill: {
    paddingVertical: 8,
    paddingHorizontal: 12,
    borderRadius: 999,
    backgroundColor: '#111',
  },
  pillLabel: {
    color: '#fff',
    fontSize: 13,
    fontWeight: '600',
  },
  emptyText: {
    color: '#999',
    fontSize: 12,
  },
});
