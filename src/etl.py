from fredapi import Fred
import pandas as pd
import os
import sqlite3
from dotenv import load_dotenv
from detect import calc_and_store_anomalies_for_indicator

load_dotenv()

fred = Fred(api_key=os.getenv("FRED_API_KEY"))
DB_PATH = 'data/macro_data.db'


INDICATORS = {
    'CPI': 'CPIAUCSL',
    'Unemployment Rate': 'UNRATE',
    'Fed Funds Rate': 'FEDFUNDS',
    'GDP': 'GDP',
    'PCE Price Index': 'PCEPI',
    'Consumer Sentiment': 'UMCSENT'
}

def fetch_fred_data():
    dfs = []
    for name, series_id in INDICATORS.items():
        try:
            print(f"Fetching data for {name}")
            data_series = fred.get_series(series_id)

            # handle cases where get_series returns None or empty series
            if data_series is None or data_series.empty:
                print(f"No data found for {name}")
                continue
            data = data_series.to_frame(name='value')
            data.index.name = 'date'
            data.reset_index(inplace=True)
            data['indicator'] = name
            dfs.append(data)
        except Exception as e:
            print(f"Error fetching data for {name} ({series_id}): {e}")
            continue
    
    if not dfs:
        print("No data fetched for any indicators. Check your API key and series IDs.")
        return pd.DataFrame()
    
    all_data = pd.concat(dfs, ignore_index=True)
    all_data['date'] = pd.to_datetime(all_data['date'].dt.strftime('%Y-%m-%d')) # Standardize date format
    return all_data[['date', 'indicator', 'value']]

def save_to_db(df):
    if df.empty:
        print("No data to save to database.")
        return False
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        df.to_sql('macro_data', conn, if_exists='replace', index=False)
        print(f"Successfully saved {len(df)} rows to macro_data table.")
        conn.close()
        return True
    except sqlite3.Error as e:
        print(f"Database error in save_to_db: {e}")
        if conn:
            conn.close()
        return False
    except Exception as e:
        print(f"An unexpected error occurred in save_to_db: {e}")
        if conn:
            conn.close()
        return False

if __name__ == '__main__':
    print("Starting ETL process...")
    df_fred = fetch_fred_data()
    
    if not df_fred.empty:
        if save_to_db(df_fred):
            print("\nStarting anomaly detection for all indicators...")
            # Get unique indicators that were actually saved
            unique_indicators = df_fred['indicator'].unique()
            for indicator_name in unique_indicators:
                print(f"Calculating anomalies for: {indicator_name}")
                calc_and_store_anomalies_for_indicator(indicator_name)
            print("\nAnomaly detection complete.")
        else:
            print("Skipping anomaly detection due to error in saving main data.")
    else:
        print("No data fetched, ETL process cannot continue to anomaly detection.")
    
    print("ETL process finished.")