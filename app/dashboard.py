import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import sqlite3

app = dash.Dash(__name__)

conn = sqlite3.connect('data/macro_data.db')
df = pd.read_sql('SELECT * FROM cpi_data', conn)
conn.close()

fig = px.line(df, x='date', y='CPI', title='CPI Over Time')

app.layout = html.Div([
    html.H1('CPI Over Time'),
    dcc.Graph(figure=fig)
])
    
if __name__ == '__main__':
    app.run(debug=True)