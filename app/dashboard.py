import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))
from detect import get_anomalies_for_indicator
from compare import build_comparison
from categories import build_grouped_options, first_real_value

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'macro_data.db')

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
        
        # YOUR TURN: add a SECOND control (a dcc.Checklist / toggle, e.g.
        # id='group-toggle') that collapses this grouped view back to a flat
        # alphabetical list. When toggled "flat", the call site would feed the
        # dropdown ``[{'label': i, 'value': i} for i in available_indicators]``
        # (or a future ``build_grouped_options(available_indicators,
        # grouped=False)`` path) instead of the grouped options below, via a
        # callback that swaps the dropdown's `options`. Comment only -- do not
        # implement; the grouped wiring below is the one-increment change.
        dcc.Dropdown(
            id='indicator-dropdown',
            options=build_grouped_options(available_indicators),
            value=first_real_value(build_grouped_options(available_indicators)),
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
        ),

        html.Hr(),
        html.H3('Compare Two Indicators'),
        html.Div([
            dcc.Dropdown(
                id='compare-dropdown-a',
                options=build_grouped_options(available_indicators),
                value=first_real_value(build_grouped_options(available_indicators)),
                clearable=False,
                style={'width': '45%', 'display': 'inline-block'}
            ),
            dcc.Dropdown(
                id='compare-dropdown-b',
                options=build_grouped_options(available_indicators),
                value=available_indicators[1] if len(available_indicators) > 1 else None,
                clearable=False,
                style={'width': '45%', 'display': 'inline-block', 'marginLeft': '2%'}
            ),
        ]),
        dcc.Checklist(
            id='compare-pct-change',
            options=[{'label': ' Show as % change', 'value': 'pct'}],
            value=[]
        ),
        dcc.Graph(id='compare-overlay-graph'),
        html.Div(id='compare-corr-summary', style={'fontWeight': 'bold'}),
        dcc.Graph(id='compare-rolling-corr-graph'),
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

def _empty_fig(title):
    """A blank Plotly figure with just a title (used as a safe placeholder)."""
    fig = go.Figure()
    fig.update_layout(title=title)
    return fig


def build_overlay_figure(comparison, name_a, name_b):
    """Turn a build_comparison() result into a dual y-axis overlay figure.

    `comparison` is the dict returned by ``compare.build_comparison`` and its
    ``overlay`` value is a date-indexed DataFrame with one column per indicator.
    The two indicators usually live on very different scales, so series A goes on
    the primary (left) y-axis and series B on a secondary (right) y-axis.
    """
    overlay = comparison['overlay']
    if overlay is None or overlay.empty:
        return _empty_fig('No overlapping dates for the selected indicators.')

    fig = go.Figure()

    # YOUR TURN: add the two overlay traces. Plot overlay[name_a] on the
    # primary y-axis and overlay[name_b] on a secondary y-axis ('y2'), then call
    # fig.update_layout(...) to define yaxis2 with overlaying='y', side='right'.
    # The data is already aligned and (optionally) %-change transformed for you;
    # build_comparison + its unit tests cover the math. ~5-8 lines. Until then we
    # show an axis-less line so the page still renders and tests stay green:
    fig.add_scatter(x=overlay.index, y=overlay[name_a], mode='lines', name=name_a)

    suffix = ' (% change)' if comparison.get('pct_change') else ''
    fig.update_layout(title=f'{name_a} vs {name_b}{suffix}')
    return fig


@app.callback(
    Output('compare-overlay-graph', 'figure'),
    Output('compare-corr-summary', 'children'),
    Output('compare-rolling-corr-graph', 'figure'),
    Input('compare-dropdown-a', 'value'),
    Input('compare-dropdown-b', 'value'),
    Input('compare-pct-change', 'value'),
)
def update_comparison(indicator_a, indicator_b, pct_change_value):
    if not indicator_a or not indicator_b:
        msg = 'Select two indicators to compare.'
        return _empty_fig(msg), msg, _empty_fig('')

    df_a = get_data_for_indicator_graph(indicator_a)
    df_b = get_data_for_indicator_graph(indicator_b)

    pct_change = bool(pct_change_value) and 'pct' in pct_change_value
    comparison = build_comparison(
        df_a, df_b, name_a=indicator_a, name_b=indicator_b, pct_change=pct_change
    )

    overlay_fig = build_overlay_figure(comparison, indicator_a, indicator_b)

    overall = comparison['overall_corr']
    if overall != overall:  # NaN check
        summary = f'Not enough overlapping data to correlate {indicator_a} and {indicator_b}.'
    else:
        summary = (
            f'Overall correlation ({comparison["n_overlap"]} shared points): '
            f'{overall:.2f}  |  rolling window: {comparison["window"]}'
        )

    roll = comparison['rolling_corr'].dropna()
    if roll.empty:
        corr_fig = _empty_fig('Rolling correlation unavailable.')
    else:
        corr_fig = px.line(
            x=roll.index, y=roll.values,
            title=f'Rolling correlation (window={comparison["window"]})',
            labels={'x': 'date', 'y': 'correlation'}
        )
        corr_fig.update_yaxes(range=[-1.05, 1.05])

    return overlay_fig, summary, corr_fig


if __name__ == '__main__':
    app.run_server(debug=True)