// SCREEN_LANDING_PAGE.js

import { FontAwesome5, MaterialIcons } from '@expo/vector-icons';
import { useNavigation } from '@react-navigation/native';
import { router } from 'expo-router';
import { useState } from 'react';
import { StyleSheet, Text, TextInput, TouchableOpacity, View } from 'react-native';
import { DEBUG_CONSOLE_LOG } from './CLIENT_APP_FUNCTIONS';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

export default function SCREEN_LANDING_PAGE() {
  const navigation = useNavigation();
  const [displayName, setDisplayName] = useState(CLIENT_APP_VARIABLES.USER_DISPLAY_NAME || '');
  const [isEditing, setIsEditing] = useState(false);

  const updateDisplayName = async () => {
    if (displayName !== CLIENT_APP_VARIABLES.USER_DISPLAY_NAME) {
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

      DEBUG_CONSOLE_LOG();

      router.push('/SCREEN_MAIN');
    } else {
      alert(`${mode} selected!`);
    }
  };

  return (
    <View style={styles.container}>
      {displayName ? (
        <Text style={styles.heading}>
          Welcome{' '}
          <TextInput
            style={styles.editableName}
            value={displayName}
            onChangeText={setDisplayName}
            onBlur={updateDisplayName}
            onSubmitEditing={updateDisplayName}
            returnKeyType="done"
          />
          !
        </Text>
      ) : (
        <Text style={styles.heading}>Welcome!</Text>
      )}

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

      <TouchableOpacity style={styles.card} onPress={() => alert('Coming soon: Connect with Musicians')}>
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
  container: {
    flex: 1,
    padding: 20,
    backgroundColor: '#fff',
  },
  heading: {
    fontSize: 28,
    fontWeight: 'bold',
    marginTop: 40,
  },
  editableName: {
    borderBottomWidth: 1,
    borderColor: '#ccc',
    fontSize: 28,
    fontWeight: 'bold',
    padding: 0,
    margin: 0,
  },
  subheading: {
    fontSize: 20,
    marginBottom: 20,
  },
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
  icon: {
    marginRight: 15,
    marginTop: 5,
  },
  textContainer: {
    flex: 1,
  },
  title: {
    fontSize: 18,
    fontWeight: 'bold',
  },
  description: {
    marginTop: 4,
    fontSize: 14,
    color: '#444',
  },
});
