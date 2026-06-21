import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
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
import alerts  # pure, scipy-free -> importing it keeps `import app.dashboard` OK offline

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
    # Build the grouped compare options once and reuse for both compare
    # dropdowns; dropdown-a defaults to the first selectable indicator and
    # dropdown-b to the first selectable indicator DISTINCT from a's default.
    _compare_options = build_grouped_options(available_indicators)
    _compare_default_a = first_real_value(_compare_options)
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
        # --- User-defined alerts panel (thin wiring over alerts.build_alerts_payload) ---
        html.H3('Alerts'),
        dcc.Dropdown(
            id='alert-indicator-dropdown',
            options=build_grouped_options(available_indicators),
            value=first_real_value(build_grouped_options(available_indicators)),
            clearable=False
        ),
        dcc.RadioItems(
            id='alert-kind',
            options=[
                {'label': ' Anomaly (rolling-z)', 'value': 'anomaly'},
                {'label': ' Level (raw value)', 'value': 'level'},
            ],
            value='anomaly',
        ),
        dcc.RadioItems(
            id='alert-direction',
            options=[
                {'label': ' Above', 'value': 'above'},
                {'label': ' Below', 'value': 'below'},
                {'label': ' Both', 'value': 'both'},
            ],
            value='both',
        ),
        # threshold min=0 sidesteps the degenerate anomaly/below double-minus
        # verb ("z=--3"); a positive z-magnitude is the conventional input.
        dcc.Input(id='alert-threshold', type='number', value=3.0, min=0,
                  placeholder='threshold'),
        dcc.Input(id='alert-window', type='number', value=12, min=1,
                  placeholder='window'),
        html.Button('Run alert', id='alert-run-button', n_clicks=0),
        html.Div(id='alert-summary', style={'fontWeight': 'bold'}),
        dash_table.DataTable(
            id='alert-table',
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
                options=_compare_options,
                value=_compare_default_a,
                clearable=False,
                style={'width': '45%', 'display': 'inline-block'}
            ),
            dcc.Dropdown(
                id='compare-dropdown-b',
                options=_compare_options,
                # Default to the first selectable indicator that is NOT the one
                # dropdown-a defaults to, so the compare view never opens
                # comparing an indicator to itself. Falls to None when there is
                # only one (or zero) indicator -- no IndexError, no self-compare.
                value=first_real_value(_compare_options, exclude=_compare_default_a),
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


@app.callback(
    Output('alert-summary', 'children'),
    Output('alert-table', 'data'),
    Input('alert-run-button', 'n_clicks'),
    State('alert-indicator-dropdown', 'value'),
    State('alert-kind', 'value'),
    State('alert-direction', 'value'),
    State('alert-threshold', 'value'),
    State('alert-window', 'value'),
)
def update_alert_panel(n_clicks, indicator, kind, direction, threshold, window):
    """Run one user-defined alert and render its summary + firing rows.

    Builds an :class:`alerts.AlertRule` from the panel inputs, fetches the
    indicator's tidy {'date','value'} frame via the EXISTING
    ``get_data_for_indicator_graph`` path, and renders the PURE
    ``alerts.build_alerts_payload`` result -- the one-line summary into
    'alert-summary' and the firing rows into 'alert-table'.
    """
    # YOUR TURN: this is the documented LIVE-DATA boundary (like the other
    # get_*_for_indicator paths). OFFLINE there are no macro_data.db rows, so
    # get_data_for_indicator_graph returns the empty {'date','value'} frame ->
    # build_alerts_payload degrades to "No alerts." + an empty table and NEVER
    # crashes. A learner who populates macro_data.db (run etl.py) sees real
    # firings here; the pure core in alerts.py is the tested part, this callback
    # is the thin hook. Do not add a DataTable test that needs a live server/DB.
    if not indicator:
        return 'No alerts.', []

    try:
        rule = alerts.AlertRule(
            indicator=indicator,
            kind=kind or 'anomaly',
            threshold=float(threshold) if threshold is not None else 3.0,
            window=int(window) if window is not None else 12,
            direction=direction or 'both',
        )
    except (ValueError, TypeError):
        # An invalid kind/direction (shouldn't happen via the RadioItems) ->
        # degrade gracefully rather than 500 the callback.
        return 'No alerts.', []

    df = get_data_for_indicator_graph(indicator)
    payload = alerts.build_alerts_payload({indicator: df}, [rule])
    result = payload[0]

    fired = result['fired'].copy()
    if 'z_score' in fired.columns:
        fired['z_score'] = fired['z_score'].round(2)
    if not fired.empty:
        fired['date'] = pd.to_datetime(fired['date']).dt.strftime('%Y-%m-%d')

    return result['summary'], fired.to_dict('records')


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

    # Series A on the primary (left) y-axis; series B on a secondary (right)
    # y-axis so two very different scales can share one chart. The data is
    # already aligned and (optionally) %-change transformed by build_comparison.
    fig.add_scatter(x=overlay.index, y=overlay[name_a], mode='lines', name=name_a)
    fig.add_scatter(
        x=overlay.index, y=overlay[name_b], mode='lines', name=name_b, yaxis='y2'
    )

    suffix = ' (% change)' if comparison.get('pct_change') else ''
    fig.update_layout(
        title=f'{name_a} vs {name_b}{suffix}',
        yaxis2=dict(overlaying='y', side='right'),
    )
    # YOUR TURN: tell the two axes apart at a glance. Give each y-axis its own
    # title (yaxis_title=name_a) and colour the right axis to match series B's
    # line -- set yaxis2's title/tickfont colour to the second trace's colour
    # (e.g. fig.data[1].line.color) so a reader knows which series each axis
    # belongs to. Comment only for now; ~3-4 lines when you fill it in.
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