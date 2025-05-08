import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import sqlite3
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))
from detect import get_anomalies_for_indicator

DB_PATH = 'data/macro_data.db'
app = dash.Dash(__name__)
server = app.server


def get_distinct_indicators():
    """Fetches distinct indicators from the macro_data table."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        # Ensure macro_data table exists before trying to read from it
        conn.execute('''
            CREATE TABLE IF NOT EXISTS macro_data (
                date TEXT,
                indicator TEXT,
                value REAL
            )
        ''')
        indicators_df = pd.read_sql_query('SELECT DISTINCT indicator FROM macro_data ORDER BY indicator', conn)
        return indicators_df['indicator'].tolist()
    except sqlite3.Error as e:
        print(f"Database error in get_distinct_indicators: {e}")
        return []
    except Exception as e:
        print(f"An error occurred in get_distinct_indicators: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_data_for_indicator_graph(indicator_name):
    """Fetches all data for a given indicator for the graph."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(f"SELECT date, value FROM macro_data WHERE indicator = ? ORDER BY date", 
                               conn, params=(indicator_name,))
        df['date'] = pd.to_datetime(df['date']) # Ensure date is in datetime format for Plotly
        return df
    except sqlite3.Error as e:
        print(f"Database error in get_data_for_indicator_graph for {indicator_name}: {e}")
        return pd.DataFrame(columns=['date', 'value'])
    except Exception as e:
        print(f"An error occurred in get_data_for_indicator_graph for {indicator_name}: {e}")
        return pd.DataFrame(columns=['date', 'value'])


# Fetch initial set of indicators for the dropdown
available_indicators = get_distinct_indicators()

# Define the layout as a function to ensure it's fresh on page load
def serve_layout():
    return html.Div([
        html.H1('FRED Macro Dashboard'),
        
        dcc.Dropdown(
            id='indicator-dropdown',
            options=[{'label': ind, 'value': ind} for ind in available_indicators],
            value=available_indicators[0] if available_indicators else None,
            clearable=False
        ),
        
        dcc.Graph(id='macro-graph'),
        
        html.H3('Detected Anomalies'),
        dash_table.DataTable(
            id='anomaly-table',
            columns=[
                {'name': 'Date', 'id': 'date'},
                {'name': 'Value', 'id': 'value'},
                {'name': 'Z-Score', 'id': 'z_score'}
            ],
            data=[],
            page_size=10,
            style_cell={'textAlign': 'left'},
            style_header={
                'backgroundColor': 'lightgrey',
                'fontWeight': 'bold'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }
            ]
        )
    ])

app.layout = serve_layout # Assign the layout function

# Callback to update the graph
@app.callback(
    Output('macro-graph', 'figure'),
    Input('indicator-dropdown', 'value')
)
def update_graph(selected_indicator):
    if not selected_indicator:
        return px.line(title="Please select an indicator.")
    
    df_graph = get_data_for_indicator_graph(selected_indicator)
    
    if df_graph.empty:
        return px.line(title=f"No data available for {selected_indicator}")

    fig = px.line(df_graph, x='date', y='value', title=f'{selected_indicator} Over Time')
    
    # Optional: Highlight anomalies on the graph
    df_anomalies = get_anomalies_for_indicator(selected_indicator)
    if not df_anomalies.empty:
        df_anomalies['date'] = pd.to_datetime(df_anomalies['date']) # Ensure date is datetime
        # Merge anomaly data with graph data to get anomaly values at specific dates
        # This assumes anomalies are a subset of the main data points
        anomalous_points = pd.merge(df_graph, df_anomalies, on='date', how='inner', suffixes=('', '_anomaly'))
        
        if not anomalous_points.empty:
            fig.add_scatter(
                x=anomalous_points['date'], 
                y=anomalous_points['value'], # Use 'value' from the merged df_graph part
                mode='markers', 
                marker=dict(color='red', size=10, symbol='x'), 
                name='Anomaly'
            )
            
    fig.update_layout(transition_duration=500) # Smooth transition
    return fig

@app.callback(
    Output('anomaly-table', 'data'),
    Output('anomaly-table', 'columns'),
    Input('indicator-dropdown', 'value')
)
def update_anomaly_table(selected_indicator):
    if not selected_indicator:
        return [], []

    anomalies_df = get_anomalies_for_indicator(selected_indicator)
    
    if anomalies_df.empty:
        return [], [{'name': 'Date', 'id': 'date'}, {'name': 'Value', 'id': 'value'}, {'name': 'Z-Score', 'id': 'z_score'}]


    # Format Z-score for better readability in the table
    if 'z_score' in anomalies_df.columns:
        anomalies_df['z_score'] = anomalies_df['z_score'].round(2)
    
    # Ensure date is string for DataTable if it's not already
    anomalies_df['date'] = pd.to_datetime(anomalies_df['date']).dt.strftime('%Y-%m-%d')


    columns = [
        {'name': 'Date', 'id': 'date', 'type': 'datetime'},
        {'name': 'Value', 'id': 'value', 'type': 'numeric'},
        {'name': 'Z-Score', 'id': 'z_score', 'type': 'numeric'}
    ]
    return anomalies_df.to_dict('records'), columns

if __name__ == '__main__':
    app.run_server(debug=True)