// SCREEN_MAIN_4_COLOR_CHART.js
import { forwardRef, useImperativeHandle, useState } from 'react';
import { StyleSheet, Text, View } from 'react-native';
import CLIENT_APP_VARIABLES from './CLIENT_APP_VARIABLES';

const SCREEN_MAIN_4_COLOR_CHART = forwardRef((props, ref) => {
  const [RESULT_SET_P_CLIENT_DD_COLOR_LEGEND, SET_RESULT_SET_P_CLIENT_DD_COLOR_LEGEND] = useState([]);

  const REFRESH = async () => {
    try {
      // Force default breakdown if null
      if (!CLIENT_APP_VARIABLES.BREAKDOWN_NAME) {
        CLIENT_APP_VARIABLES.BREAKDOWN_NAME = 'OVERALL';
      }

      console.log('🎨 Fetching Color Legend with params:', {
        VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
        BREAKDOWN_NAME: CLIENT_APP_VARIABLES.BREAKDOWN_NAME,
      });

      const res = await fetch(`${CLIENT_APP_VARIABLES.BACKEND_URL}/CALL_SP`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          SP_NAME: 'P_CLIENT_DD_COLOR_LEGEND',
          PARAMS: {
            VIOLINIST_ID: CLIENT_APP_VARIABLES.VIOLINIST_ID,
            BREAKDOWN_NAME: CLIENT_APP_VARIABLES.BREAKDOWN_NAME,
          },
        }),
      });

      const json = await res.json();
      const SORTED_RESULT = (json.RESULT || []).sort(
        (a, b) => a.Display_Order_No - b.Display_Order_No
      );
      SET_RESULT_SET_P_CLIENT_DD_COLOR_LEGEND(SORTED_RESULT);

      console.log(`✅ Color Legend loaded: ${SORTED_RESULT.length} records`);
    } catch (err) {
      console.error('❌ [ColorChart] Error fetching color legend:', err);
    }
  };

  // Expose REFRESH() to parent
  useImperativeHandle(ref, () => ({ REFRESH }));

  return (
    <View style={styles.legendWrapper}>
      {RESULT_SET_P_CLIENT_DD_COLOR_LEGEND.length > 0 ? (
        <View style={styles.legendRowInline}>
          {RESULT_SET_P_CLIENT_DD_COLOR_LEGEND.map((item, idx) => {
            const rgba = item.Color_RGBA.replace(/[()]/g, '').split(',');
            const color = `rgba(${rgba.join(',')})`;
            return (
              <View key={idx} style={styles.legendItemInline}>
                <View style={[styles.legendDot, { backgroundColor: color }]} />
                <Text style={styles.legendText}>{item.Display_Name}</Text>
              </View>
            );
          })}
        </View>
      ) : null}
    </View>
  );
});

export default SCREEN_MAIN_4_COLOR_CHART;

const styles = StyleSheet.create({
  legendWrapper: {
    alignItems: 'center',
    justifyContent: 'center',
  },
  legendRowInline: {
    flexDirection: 'row',
    alignItems: 'center',
    flexWrap: 'wrap',
    justifyContent: 'center',
  },
  legendItemInline: {
    flexDirection: 'row',
    alignItems: 'center',
    marginRight: 16,
    marginBottom: 4,
  },
  legendDot: {
    width: 14,
    height: 14,
    borderRadius: 7,
    marginRight: 6,
  },
  legendText: {
    fontSize: 14,
    color: '#444',
  },
});
