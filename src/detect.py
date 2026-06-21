# detect.py
"""Anomaly detection for indicator series.

The scoring core is a PURE numpy/pandas *rolling* z-score -- no scipy (its
native ``_propack`` extension fails to dlopen in this environment) and no
database/network dependency, so the helpers below can be unit-tested against
synthetic data. A value is flagged when it deviates from its *local* trailing
window rather than from a single global mean/std over the whole series, which
catches regime-relative spikes a global z-score misses.
"""
import numpy as np
import pandas as pd
import sqlite3

DB_PATH = 'data/macro_data.db'


def rolling_zscore(values, window=12, min_periods=None):
    """Trailing rolling z-score of a 1-D numeric series (pure numpy/pandas).

    Each point is scored against the mean/std of the ``window`` observations up
    to and including it, so the result reflects how unusual a value is relative
    to its *recent* neighborhood rather than the whole sample.

    Parameters
    ----------
    values : pandas.Series or 1-D array-like
        Coerced to numeric with ``errors='coerce'`` (non-numbers -> NaN).
    window : int
        Number of trailing observations in each rolling window. The effective
        window is clamped to ``max(2, min(window, len(s)))`` -- the same way
        compare.py clamps its rolling window -- so a window wider than the data
        does not blow up on short series.
    min_periods : int or None
        Minimum observations required to emit a (non-NaN) score. Defaults to the
        effective window, so the first ``eff_window - 1`` rows are NaN (not yet
        enough local history) instead of being scored off a tiny sample.

    Returns
    -------
    pandas.Series
        Float z-scores aligned to the input's index. Empty in -> empty out.

    Notes
    -----
    A flat / zero-variance trailing window makes the rolling std 0, so
    ``(s - mean) / std`` is 0/0 -> NaN (and could be +/-inf where a deviation
    meets a 0 std). Infinities are scrubbed to NaN; a NaN z-score means
    "not enough information / no variance", which downstream code treats as
    *not* an anomaly (NaN never compares ``> threshold``).
    """
    s = values if isinstance(values, pd.Series) else pd.Series(values)
    # Coerce first (errors='coerce' turns non-numbers into NaN) THEN cast, so a
    # string array-like does not raise on construction.
    s = pd.to_numeric(s, errors='coerce').astype('float64')

    n = len(s)
    if n == 0:
        return pd.Series(dtype='float64', index=s.index, name='z_score')

    eff_window = max(2, min(window, n))
    eff_min_periods = eff_window if min_periods is None else min_periods

    roll = s.rolling(window=eff_window, min_periods=eff_min_periods)
    std = roll.std(ddof=0)
    z = (s - roll.mean()) / std
    # Scrub 0/0 -> NaN and (deviation)/0 -> +/-inf; both mean "no usable local
    # context", so they must not register as anomalies.
    z = z.replace([np.inf, -np.inf], np.nan)
    z.name = 'z_score'
    return z


def detect_anomalies(values, window=12, z_threshold=3.0, min_periods=None):
    """Flag local anomalies via the rolling z-score.

    Wraps :func:`rolling_zscore` and returns a DataFrame with the original
    ``value``, its rolling ``z_score`` (NaN where there is not enough local
    history or the window has zero variance), and a boolean ``is_outlier`` mask
    that is True only where ``abs(z) > z_threshold``. NaN z-scores compare
    False, so a flat / short window never produces a spurious anomaly.

    The ``z_score`` column is what gets persisted into the existing
    ``anomaly_data.z_score`` field.
    """
    z = rolling_zscore(values, window=window, min_periods=min_periods)
    s = pd.to_numeric(
        values if isinstance(values, pd.Series) else pd.Series(values),
        errors='coerce',
    ).astype('float64')
    s.index = z.index  # keep value + z aligned even for raw array-like input

    out = pd.DataFrame({'value': s, 'z_score': z})
    # abs() of NaN is NaN, and NaN > threshold is False -> not an outlier.
    out['is_outlier'] = out['z_score'].abs() > z_threshold
    return out


def calc_and_store_anomalies_for_indicator(indicator_name, z_threshold=3, window=12):
    """
    Calculates Z-score anomalies for a specific indicator and stores them in the anomaly_data table.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # ** Ensure anomaly_data table exists right at the beginning **
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS anomaly_data (
                date TEXT,
                indicator TEXT,
                value REAL,
                z_score REAL,
                PRIMARY KEY (date, indicator)
            )
        ''')
        conn.commit()

        # Fetch data for the specific indicator
        # Using pd.read_sql_query for safety with parameters
        df_indicator_data = pd.read_sql_query(
            "SELECT date, value FROM macro_data WHERE indicator = ? ORDER BY date",
            conn,
            params=(indicator_name,)
        )

        if df_indicator_data.empty or df_indicator_data['value'].isnull().all():
            print(f"No data or all null values for indicator: {indicator_name}. Skipping anomaly detection.")
            # Still ensure old anomalies for this indicator are cleared
            cursor.execute("DELETE FROM anomaly_data WHERE indicator = ?", (indicator_name,))
            conn.commit()
            return pd.DataFrame()

        df_indicator_data['value'] = pd.to_numeric(df_indicator_data['value'], errors='coerce')
        df_indicator_data.dropna(subset=['value'], inplace=True)

        if len(df_indicator_data) < 2:
            print(f"Not enough data points for Z-score calculation for indicator: {indicator_name}.")
            cursor.execute("DELETE FROM anomaly_data WHERE indicator = ?", (indicator_name,))
            conn.commit()
            return pd.DataFrame()

        # Score each point against its LOCAL trailing window (pure numpy/pandas;
        # no scipy). detect_anomalies scrubs inf and leaves NaN where a window is
        # flat or too short, and NaN never registers as an outlier.
        scored = detect_anomalies(
            df_indicator_data['value'],
            window=window,
            z_threshold=z_threshold,
        )
        df_indicator_data['z_score'] = scored['z_score'].values
        df_indicator_data['is_outlier'] = scored['is_outlier'].values

        # NaN z-scores (no local context / zero variance) are not anomalies and
        # must not be persisted as spurious rows.
        outliers_df = df_indicator_data[df_indicator_data['is_outlier']].copy()
        
        # Remove old anomalies for the current indicator before inserting new ones
        cursor.execute("DELETE FROM anomaly_data WHERE indicator = ?", (indicator_name,))
        conn.commit() # Commit the delete

        if not outliers_df.empty:
            outliers_df['indicator'] = indicator_name
            outliers_to_store = outliers_df[['date', 'indicator', 'value', 'z_score']]
            
            # Insert new anomalies
            outliers_to_store.to_sql('anomaly_data', conn, if_exists='append', index=False)
            conn.commit() # Commit the insert
            print(f"Stored {len(outliers_to_store)} anomalies for {indicator_name}.")
            return outliers_to_store
        else:
            print(f"No new anomalies found for {indicator_name}. Old anomalies (if any) cleared.")
            return pd.DataFrame()

    except sqlite3.Error as e:
        print(f"Database error in calculate_and_store_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred in calculate_and_store_anomalies_for_indicator for {indicator_name}: {e}")
        # Print full traceback for unexpected errors during debugging
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_anomalies_for_indicator(indicator_name):
    """
    Fetches stored anomalies for a specific indicator.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # Ensure anomaly_data table exists before trying to read from it
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS anomaly_data (
                date TEXT,
                indicator TEXT,
                value REAL,
                z_score REAL,
                PRIMARY KEY (date, indicator)
            )
        ''')
        conn.commit()
        anomalies_df = pd.read_sql_query("SELECT date, value, z_score FROM anomaly_data WHERE indicator = ?", 
                                         conn, params=(indicator_name,))
        return anomalies_df
    except sqlite3.Error as e:
        print(f"Database error in get_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame(columns=['date', 'value', 'z_score'])
    except Exception as e:
        print(f"An error occurred in get_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame(columns=['date', 'value', 'z_score'])
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    try:
        conn_main = sqlite3.connect(DB_PATH)
        # Check if macro_data table exists
        table_check_cursor = conn_main.cursor()
        table_check_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='macro_data'")
        if table_check_cursor.fetchone() is None:
            print("Error: 'macro_data' table does not exist. Run etl.py to create and populate it first.")
            conn_main.close()
        else:
            indicators_in_db = pd.read_sql_query("SELECT DISTINCT indicator FROM macro_data", conn_main)['indicator'].tolist()
            conn_main.close()

            if not indicators_in_db:
                print("No indicators found in macro_data table. Run etl.py first (ensure it saves data).")
            else:
                print(f"Found indicators: {indicators_in_db}")
                for indicator in indicators_in_db:
                    print(f"\nProcessing anomalies for: {indicator}")
                    calculate_and_store_anomalies_for_indicator(indicator)
                print("\nFinished processing anomalies for all indicators.")
    except sqlite3.Error as e:
        print(f"Database error in detect.py __main__: {e}")
    except Exception as e:
        print(f"Error in detect.py __main__: {e}")
        import traceback
        traceback.print_exc()