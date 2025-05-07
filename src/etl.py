from fredapi import Fred
import pandas as pd
import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

fred = Fred(api_key=os.getenv("FRED_API_KEY"))

INDICATORS = {
    'CPI': 'CPIAUCSL',
    'Unemployment Rate': 'UNRATE',
    'Fed Funds Rate': 'FEDFUNDS'
}

def fetch_fred_data():
    dfs = []
    for name, series_id in INDICATORS.items():
        data = fred.get_series(series_id).to_frame(name='value')
        data.index.name = 'date'
        data.reset_index(inplace=True)
        data['indicator'] = name
        dfs.append(data)
    all_data = pd.concat(dfs, ignore_index=True)
    return all_data[['date', 'indicator', 'value']]

def save_to_db(df):
    conn = sqlite3.connect('data/macro_data.db')
    df.to_sql('macro_data', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == '__main__':
    df = fetch_fred_data()
    save_to_db(df)