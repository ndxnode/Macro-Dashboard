from scipy.stats import zscore
import pandas as pd
import sqlite3

DB_PATH = 'data/macro_data.db'

def calc_and_store_anomalies_for_indicator(indicator, z_threshold=3):
    """Calculate and store anomalies for a specific indicator in the anomalies table."""

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        # Read data from the macro_data table for the given indicator
        df = pd.read_sql_query(f"SELECT date, value FROM macro_data WHERE indicator = ?", conn, params=(indicator,))

        if df.empty or df['value'].isnull().all():
            print(f"No data found for indicator: {indicator}. Skipping anomaly detection.")
            return pd.DataFrame()

        # Convert value to numeric and drop rows with missing values
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df.dropna(subset=['value'], inplace=True)
        
        if len(df) < 2: # Not enough data to calculate z-scores
            print(f"Not enough data for indicator: {indicator}. Skipping anomaly detection.")
            return pd.DataFrame()
        
        df['z_score'] = zscore(df['value'])
        df["is_outlier"] = df["z_score"].abs() > z_threshold

        outliers = df[df['is_outlier']].copy() # Copy to avoid SettingWithCopyWarning

        if not outliers.empty:
            outliers.df['indicator'] = indicator_name # Add indicator name to storage
            # select and rename cols for anomaly table
            outliers_to_store = outliers_df[['date', 'indicator', 'value', 'z_score']]

            # Store outliers in the anomaly_data table
            # Use a temporary table and then insert or replace to avoid issues with partial writes
            # And to handle cases where the table might already exist with old anomalies for this indicator
            outliers_to_store.to_sql('temp_anomaly_data', conn, if_exists='append', index=False)    

            # Ensure anomaly_data table exists
            conn.execute('''
                CREATE TABLE IF NOT EXISTS anomaly_data (
                    date TEXT,
                    indicator TEXT,
                    value REAL,
                    z_score REAL,
                    PRIMARY KEY (date, indicator)
                )
            ''')

            # Remove old anomalies for the current indicator before inserting new ones
            # This prevents duplicate entries if script is run multiple times
            cursor = conn.cursor()
            cursor.execute("DELETE FROM anomaly_data WHERE indicator = ?", (indicator_name,))
            
            # Insert new anomalies
            # Using pandas to_sql with if_exists='append' after deleting is simpler
            outliers_to_store.to_sql('anomaly_data', conn, if_exists='append', index=False)
            conn.commit()
            print(f"Stored {len(outliers_to_store)} anomalies for {indicator_name}.")
            return outliers_to_store
        else:
            # If no outliers, still ensure old ones for this indicator are cleared
            cursor = conn.cursor()
            cursor.execute("DELETE FROM anomaly_data WHERE indicator = ?", (indicator_name,))
            conn.commit()
            print(f"No new anomalies found for {indicator_name}. Cleared any old ones.")
            return pd.DataFrame() # Return empty DataFrame

    except sqlite3.Error as e:
        print(f"Database error in calc_and_store_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame() # Return empty DataFrame in case of error
    except Exception as e:
        print(f"An error occurred in calc_and_store_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame() # Return empty DataFrame in case of error
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
        # Ensure anomaly_data table exists before trying to read from it
        conn.execute('''
            CREATE TABLE IF NOT EXISTS anomaly_data (
                date TEXT,
                indicator TEXT,
                value REAL,
                z_score REAL,
                PRIMARY KEY (date, indicator)
            )
        ''')
        anomalies_df = pd.read_sql_query(f"SELECT date, value, z_score FROM anomaly_data WHERE indicator = ?", 
                                         conn, params=(indicator_name,))
        return anomalies_df
    except sqlite3.Error as e:
        print(f"Database error in get_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame(columns=['date', 'value', 'z_score']) # Return empty DataFrame with expected columns
    except Exception as e:
        print(f"An error occurred in get_anomalies_for_indicator for {indicator_name}: {e}")
        return pd.DataFrame(columns=['date', 'value', 'z_score'])
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    try:
        conn_main = sqlite3.connect(DB_PATH)
        indicators_in_db = pd.read_sql_query("SELECT DISTINCT indicator FROM macro_data", conn_main)['indicator'].tolist()
        conn_main.close()

        if not indicators_in_db:
            print("No indicators found in macro_data table. Run etl.py first.")
        else:
            print(f"Found indicators: {indicators_in_db}")
            for indicator in indicators_in_db:
                print(f"\nProcessing anomalies for: {indicator}")
                calculate_and_store_anomalies_for_indicator(indicator)
            print("\nFinished processing anomalies for all indicators.")
            # Example of fetching anomalies for one indicator
            # test_anomalies = get_anomalies_for_indicator('CPI')
            # print("\nFetched CPI anomalies:")
            # print(test_anomalies)

    except Exception as e:
        print(f"Error in detect.py __main__: {e}")