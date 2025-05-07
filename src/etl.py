from fredapi import Fred
import pandas as pd
import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

fred = Fred(api_key=os.getenv("FRED_API_KEY"))

def get_cpi_data():
    df = fred.get_series('CPIAUCSL').to_frame(name='CPI')
    df.index.name = 'date'
    df.reset_index(inplace=True)
    return df

def save_to_db(df):
    conn = sqlite3.connect('data/macro_data.db')
    df.to_sql('cpi_data', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == '__main__':
    df = get_cpi_data()
    save_to_db(df)