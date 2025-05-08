# detect.py
from scipy.stats import zscore
import pandas as pd
import sqlite3

DB_PATH = 'data/macro_data.db'

def calc_and_store_anomalies_for_indicator(indicator_name, z_threshold=3):
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

        # Calculate Z-score. Handle potential for all values being the same (std dev = 0)
        try:
            # zscore might produce NaNs if std is 0, or runtime warnings.
            # We can pre-check or handle the output.
            if df_indicator_data['value'].std() == 0:
                 df_indicator_data['z_score'] = 0.0 # Or handle as no anomalies if all values are identical
            else:
                df_indicator_data['z_score'] = zscore(df_indicator_data['value'])
        except Exception as e_zscore:
            print(f"Warning: Could not calculate z-score for {indicator_name}, possibly due to uniform values or other error: {e_zscore}")
            df_indicator_data['z_score'] = 0.0 # Assign a default or skip

        df_indicator_data['is_outlier'] = df_indicator_data['z_score'].abs() > z_threshold
        
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