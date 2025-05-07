from scipy.stats import zscore
import pandas as pd
import sqlite3

def detect_outliers():
    conn = sqlite3.connect('data/macro_data.db')
    df = pd.read_sql('SELECT * FROM cpi_data', conn)
    conn.close()
    
    df['z_score'] = zscore(df['CPI'])
    df["outlier"] = df["z_score"].abs() > 3
    return df[df['outlier']]
    
