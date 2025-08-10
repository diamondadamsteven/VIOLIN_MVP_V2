// SCREEN_LANDING_PAGE.js
import { FontAwesome5, MaterialIcons } from '@expo/vector-icons';
import { router } from 'expo-router';
import { useMemo, useState } from 'react';
import { Dimensions, StyleSheet, Text, TextInput, TouchableOpacity, View } from 'react-native';
import { DEBUG_CONSOLE_LOG } from './CLIENT_APP_FUNCTIONS';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_LANDING_PAGE() {
  const [displayName, setDisplayName] = useState(CLIENT_APP_VARIABLES.USER_DISPLAY_NAME || '');
  const [isEditing, setIsEditing] = useState(false);

  const updateDisplayName = async () => {
    setIsEditing(false);
    if (displayName === CLIENT_APP_VARIABLES.USER_DISPLAY_NAME) return;
    CLIENT_APP_VARIABLES.USER_DISPLAY_NAME = displayName;
    try {
      await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_VIOLINIST_UPD',
          PARAMS: {
            VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
            USER_DISPLAY_NAME: displayName,
          },
        }),
      });
    } catch (error) {
      console.error('Failed to update USER_DISPLAY_NAME:', error);
    }
  };

  const handleSelection = (mode) => {
    CLIENT_APP_VARIABLES.COMPOSE_PLAY_OR_PRACTICE = mode;
    if (mode === 'Play') {
      router.push('/SCREEN_SONG_SEARCH');
    } else if (mode === 'Compose') {
      const now = new Date();
      const dateTimeString = now.toLocaleString();
      const title = `New Composition on ${dateTimeString}`;

      CLIENT_APP_VARIABLES.SONG_ID = null;
      CLIENT_APP_VARIABLES.RECORDING_ID = null;
      CLIENT_APP_VARIABLES.SONG_NAME = title;
      CLIENT_APP_VARIABLES.SHARE_LEVEL = 'PRIVATE';
      CLIENT_APP_VARIABLES.SHARE_WITH_VIOLINIST_ID = null;
      CLIENT_APP_VARIABLES.AUDIO_STREAM_FILE_NAME = null;
      CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'SUMMARY';
      DEBUG_CONSOLE_LOG();

      router.push('/SCREEN_MAIN');
    } else {
      alert(`${mode} selected!`);
    }
  };

  // Auto-size based on length
  const screenW = Dimensions.get('window').width;
  const greetingFontSize = useMemo(() => {
    const max = 34;
    const min = 20;
    const usable = screenW - 40;
    const textLength = (`Welcome ${displayName || ''}!`).length;
    const est = Math.floor(usable / (textLength * 0.55));
    return Math.max(min, Math.min(max, est));
  }, [displayName, screenW]);

  return (
    <View style={styles.container}>
      {/* Entire greeting in one baseline */}
      <View style={styles.headingRow}>
        {isEditing ? (
          <TextInput
            style={[styles.headingText, { fontSize: greetingFontSize }]}
            value={displayName}
            onChangeText={setDisplayName}
            onBlur={updateDisplayName}
            onSubmitEditing={updateDisplayName}
            returnKeyType="done"
            autoFocus
          />
        ) : (
          <TouchableOpacity onPress={() => setIsEditing(true)} activeOpacity={0.7}>
            <Text
              style={[styles.headingText, { fontSize: greetingFontSize }]}
              numberOfLines={1}
              adjustsFontSizeToFit
              minimumFontScale={0.75}
              ellipsizeMode="tail"
            >
              {`Welcome ${displayName || 'Friend'}!`}
            </Text>
          </TouchableOpacity>
        )}
      </View>

      <Text style={styles.subheading}>What would you like to do?</Text>

      <TouchableOpacity style={styles.card} onPress={() => handleSelection('Compose')}>
        <FontAwesome5 name="music" size={24} style={styles.icon} />
        <View style={styles.textContainer}>
          <Text style={styles.title}>Compose</Text>
          <Text style={styles.description}>
            Play your instrument and we'll transpose your playing into music notation,
            which you can export and edit in any music notation editor.
          </Text>
        </View>
      </TouchableOpacity>

      <TouchableOpacity style={styles.card} onPress={() => handleSelection('Play')}>
        <MaterialIcons name="play-circle-outline" size={26} style={styles.icon} />
        <View style={styles.textContainer}>
          <Text style={styles.title}>Play</Text>
          <Text style={styles.description}>
            Play a song of your choice, and we'll analyze and score your pitch, timing,
            vibrato, and more. See how you compare to other musicians.
          </Text>
        </View>
      </TouchableOpacity>

      <TouchableOpacity style={styles.card} onPress={() => router.push('/SCREEN_NETWORKING')}>
        <FontAwesome5 name="user-friends" size={24} style={styles.icon} />
        <View style={styles.textContainer}>
          <Text style={styles.title}>Connect with Musicians</Text>
          <Text style={styles.description}>
            Share compositions and recordings with your students or teachers. Connect with
            similar-level or professional-level musicians.
          </Text>
        </View>
      </TouchableOpacity>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 20, backgroundColor: '#fff' },
  headingRow: { marginTop: 40 },
  headingText: {
    fontWeight: 'bold',
    color: '#111',
  },
  subheading: { fontSize: 20, marginBottom: 20, marginTop: 8 },
  card: {
    flexDirection: 'row',
    backgroundColor: '#f8f8f8',
    padding: 15,
    borderRadius: 12,
    marginBottom: 15,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.1,
    shadowRadius: 5,
    elevation: 3,
  },
  icon: { marginRight: 15, marginTop: 5 },
  textContainer: { flex: 1 },
  title: { fontSize: 18, fontWeight: 'bold' },
  description: { marginTop: 4, fontSize: 14, color: '#444' },
});
