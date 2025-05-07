import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import sqlite3

app = dash.Dash(__name__)

def get_indicators():
    conn = sqlite3.connect('data/macro_data.db')
    indicators = pd.read_sql('SELECT DISTINCT indicator FROM macro_data', conn)['indicator'].tolist()
    conn.close()
    return indicators

def get_data_for_indicator(indicator):
    conn = sqlite3.connect('data/macro_data.db')
    df = pd.read_sql(f"SELECT date, value FROM macro_data WHERE indicator = ?", conn, params=(indicator,))
    conn.close()
    return df

indicators = get_indicators()

def serve_layout():
    return html.Div([
        html.H1('FRED Macro Dashboard'),
        dcc.Dropdown(
            id='indicator-dropdown',
            options=[{'label': ind, 'value': ind} for ind in indicators],
            value=indicators[0] if indicators else None
        ),
        dcc.Graph(id='macro-graph')
    ])

app.layout = serve_layout

from dash.dependencies import Input, Output

@app.callback(
    Output('macro-graph', 'figure'),
    Input('indicator-dropdown', 'value')
)
def update_graph(selected_indicator):
    if not selected_indicator:
        return {}
    df = get_data_for_indicator(selected_indicator)
    fig = px.line(df, x='date', y='value', title=f'{selected_indicator} Over Time')
    return fig

    
if __name__ == '__main__':
    app.run(debug=True)