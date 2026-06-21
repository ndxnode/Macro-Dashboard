# export.py
"""CSV export, date-range-preset clipping, and a pure overlay figure builder.

This is a PURE numpy/pandas module with no database or network dependency (no
``import sqlite3`` at module top, no FRED pull, no scipy), the same discipline as
compare.py / detect.py / alerts.py. It operates on the same date-indexed
"overlay" frame that :func:`compare.build_comparison` returns -- a DataFrame
indexed by date with one column per series.

Plotly is imported LAZILY inside :func:`build_export_figure` (never at module
top) and ``dcc`` is imported LAZILY inside :func:`make_download_callback`, so the
CSV / clip / filename core and a plain ``import export`` stay green and pull in
no plotly / dash / sqlite. The actual file download is a THIN registered Dash
callback (see :func:`make_download_callback`, which takes the dashboard's own
data-fetch + build_comparison fns by injection so this module never imports
dashboard): the payload / figure / filename builders here are pure. (Note:
turning a figure into PNG bytes via ``fig.to_image('png')`` needs the kaleido
engine, which is ABSENT in this environment, so that rasterization stays a
UI-side / ``# YOUR TURN:`` boundary -- it is never called here.)
"""
import io

import numpy as np
import pandas as pd


def build_csv_payload(frame, index_label='date'):
    """Return `frame` as a CSV STRING ready to hand to a browser download.

    Pure: serializes the date-indexed overlay frame with its index included,
    relabeling the index column to `index_label` so the date column is named.
    The caller's frame is NOT mutated. This is the string the Dash
    ``dcc.Download`` callback feeds to ``dcc.send_string``.

    An empty or None frame returns the empty string ``''`` -- the simplest thing
    to test, and ``dcc.send_string('')`` is harmless. (A header-only payload is
    the alternative; empty-string is chosen for simplicity.)
    """
    if frame is None or frame.empty:
        return ''
    # Name the index in a copy so the caller's frame is untouched.
    out = frame.copy()
    out.index = out.index.rename(index_label)
    return out.to_csv(index=True)


def build_export_figure(frame, title=None):
    """Return a ready-to-plot Plotly ``go.Figure`` of the overlay frame.

    Pure: one ``go.Scatter`` line trace per column of the (date-indexed) frame,
    with x=index, y=column, name=str(column). `title` is applied via
    ``update_layout`` when given. An empty or None frame returns an empty
    ``go.Figure()`` (no crash).

    Plotly is imported HERE (lazily), not at module top, so the rest of the
    module has no plotly dependency. This builder does NOT rasterize: there is no
    ``fig.to_image`` / ``fig.write_image`` call (that needs kaleido, absent here);
    turning the returned figure into PNG bytes is a UI-side concern.
    """
    import plotly.graph_objects as go  # lazy: keeps the pure core plotly-free

    fig = go.Figure()
    if frame is None or frame.empty:
        return fig

    for col in frame.columns:
        fig.add_trace(
            go.Scatter(
                x=frame.index,
                y=frame[col],
                name=str(col),
                mode='lines',
            )
        )
    if title is not None:
        fig.update_layout(title=title)
    return fig


def clip_to_last_years(frame, years):
    """Clip a datetime-indexed frame to its last `years` of data (preset helper).

    Pure date-range slice for the "1Y / 5Y / 10Y / Max" preset buttons. The
    cutoff is computed off the frame's OWN max date
    (``frame.index.max() - pd.DateOffset(years=years)``), NOT "today", so the
    result is deterministic and unit-testable; rows with ``index >= cutoff`` are
    kept.

    ``years is None`` or ``years <= 0`` is the identity / "Max" case: the frame
    is returned unchanged (a copy, so callers can't accidentally alias). An empty
    frame, or a frame whose index is not a ``DatetimeIndex`` (guarded with an
    explicit ``isinstance`` check rather than try/except), is returned as-is with
    no crash.
    """
    if frame is None:
        return frame
    if years is None or years <= 0:
        # "Max": identity. Return a copy so the caller can't alias our frame.
        return frame.copy() if hasattr(frame, 'copy') else frame
    if frame.empty or not isinstance(frame.index, pd.DatetimeIndex):
        return frame
    # pd.DateOffset(years=...) rejects non-integer years with an opaque dateutil
    # ValueError, so coerce to a whole-year window (truncating toward zero). The
    # 1Y/5Y/10Y presets pass ints already; this only hardens a fractional float
    # so the helper degrades gracefully instead of crashing, matching the empty/
    # non-datetime "return as-is, never raise" discipline above.
    cutoff = frame.index.max() - pd.DateOffset(years=int(years))
    return frame[frame.index >= cutoff]


_SAFE_FILENAME_CHARS = set(
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-'
)


def _sanitize_filename_part(part):
    """Make one filename component filesystem-safe.

    ``str()`` the part, then replace any char NOT in ``[A-Za-z0-9._-]`` with an
    underscore. Returns ``''`` for a None / empty / whitespace-only part so the
    caller can drop it.
    """
    if part is None:
        return ''
    text = str(part).strip()
    if not text:
        return ''
    return ''.join(c if c in _SAFE_FILENAME_CHARS else '_' for c in text)


def preset_years(preset):
    """Map a date-range preset TOKEN to a `years` arg for clip_to_last_years.

    Pure, total lookup for the 1Y / 5Y / 10Y / Max preset buttons. ``'1Y'`` -> 1,
    ``'5Y'`` -> 5, ``'10Y'`` -> 10; ``'Max'``, ``None``, and any unknown token ->
    ``None`` (the identity / no-clip case clip_to_last_years treats as "Max").
    Tokens are plain strings stored in a ``dcc.Store``; lookup is case-sensitive
    on the canonical tokens, falling through to ``None`` for anything else so the
    overlay is simply left unclipped rather than crashing.
    """
    return {'1Y': 1, '5Y': 5, '10Y': 10}.get(preset, None)


def download_filename(name_a, name_b=None, preset=None, ext='csv'):
    """Build a deterministic, filesystem-safe download filename.

    Joins the sanitized series names with a ``vs`` connector and an optional
    preset token using underscores, e.g. ``download_filename('GDP', 'CPI', '5Y')``
    -> ``'GDP_vs_CPI_5Y.csv'`` and ``download_filename('GDP')`` -> ``'GDP.csv'``
    (no connector when there is only one series).

    Each part is run through :func:`_sanitize_filename_part` (``str()`` then any
    char outside ``[A-Za-z0-9._-]`` becomes ``_``); empty / None parts are
    dropped. The ``preset`` token is appended only when truthy. When NO usable
    name part survives, falls back to ``'export'``. The extension is appended as
    ``'.' + ext`` (``ext`` itself is sanitized too).
    """
    names = [
        p for p in (_sanitize_filename_part(name_a), _sanitize_filename_part(name_b))
        if p
    ]
    # Two real series read as "A vs B"; a single series is just its own name.
    parts = ['_vs_'.join(names)] if names else []
    if preset:
        token = _sanitize_filename_part(preset)
        if token:
            parts.append(token)
    stem = '_'.join(parts) if parts else 'export'
    safe_ext = _sanitize_filename_part(ext) or 'csv'
    return f'{stem}.{safe_ext}'


def make_download_callback(app, fetch_fn, build_comparison_fn):
    """Wire a Dash ``dcc.Download`` to a clipped-overlay CSV on a button click.

    Registers ONE ``@app.callback`` (the dashboard calls this once after the app
    and its other callbacks are defined). ``fetch_fn`` and ``build_comparison_fn``
    are injected by the dashboard (its own ``get_data_for_indicator_graph`` and
    ``compare.build_comparison``) so this module never imports dashboard / sqlite
    / plotly at module top -- only ``dcc`` is imported LAZILY inside, keeping a
    plain ``import export`` clean.

    The callback rebuilds the compare overlay exactly as ``update_comparison``
    does, clips it to the selected preset (``compare-range-store``) BEFORE
    serializing so the CSV matches the chart, and returns
    ``dcc.send_string(build_csv_payload(clipped), download_filename(a, b,
    preset))``. If a/b are missing or the overlay is empty, build_csv_payload
    returns ``''`` and ``dcc.send_string('', filename)`` is harmless.

    The PNG variant (``dcc.send_bytes(fig.to_image('png'))``) stays a
    ``# YOUR TURN:`` hook -- kaleido is absent in this environment.
    """
    from dash import dcc  # lazy: keeps the pure core dash-free at module top
    from dash.dependencies import Input, Output, State

    @app.callback(
        Output('compare-download', 'data'),
        Input('compare-download-button', 'n_clicks'),
        State('compare-dropdown-a', 'value'),
        State('compare-dropdown-b', 'value'),
        State('compare-pct-change', 'value'),
        State('compare-range-store', 'data'),
        prevent_initial_call=True,
    )
    def _download_csv(n_clicks, indicator_a, indicator_b, pct_change_value, range_token):
        preset_token = range_token
        filename = download_filename(indicator_a, indicator_b, preset_token)
        if not indicator_a or not indicator_b:
            # Nothing to compare yet -> harmless empty payload.
            return dcc.send_string('', filename)

        df_a = fetch_fn(indicator_a)
        df_b = fetch_fn(indicator_b)
        pct_change = bool(pct_change_value) and 'pct' in pct_change_value
        comparison = build_comparison_fn(
            df_a, df_b, name_a=indicator_a, name_b=indicator_b, pct_change=pct_change
        )
        # Clip the overlay the SAME way the chart does so the CSV matches it.
        clipped = clip_to_last_years(comparison['overlay'], preset_years(preset_token))
        # YOUR TURN: a PNG download would build_export_figure(clipped) then
        # dcc.send_bytes(fig.to_image('png'), ...) -- kaleido is absent here,
        # so the static-image variant stays a comment-only hook.
        return dcc.send_string(build_csv_payload(clipped), filename)

    return _download_csv
