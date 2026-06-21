# export.py
"""CSV export, date-range-preset clipping, and a pure overlay figure builder.

This is a PURE numpy/pandas module with no database or network dependency (no
``import sqlite3`` at module top, no FRED pull, no scipy), the same discipline as
compare.py / detect.py / alerts.py. It operates on the same date-indexed
"overlay" frame that :func:`compare.build_comparison` returns -- a DataFrame
indexed by date with one column per series.

Plotly is imported LAZILY inside :func:`build_export_figure` (never at module
top), so the CSV / clip core and a plain ``import export`` stay green even if
plotly were ever missing. The actual file download and any static PNG write stay
THIN UI-side wrappers (see :func:`make_download_callback`): the payload/figure
builders here are pure. (Note: turning a figure into PNG bytes via
``fig.to_image('png')`` needs the kaleido engine, which is ABSENT in this
environment, so that rasterization stays a UI-side / ``# YOUR TURN:`` boundary --
it is never called here.)
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


def make_download_callback(app):
    """Wire a Dash dcc.Download to build_csv_payload on a button click.
    # YOUR TURN: register an @app.callback(Output(download, 'data'),
    # Input(btn, 'n_clicks'), ...) that returns dcc.send_string(
    # build_csv_payload(current_frame), 'export.csv'). Keep this the ONLY
    # I/O boundary; the payload/figure builders stay pure. No test yet.
    # (A PNG variant would need kaleido -- absent in this env -- so it stays
    # a UI-side dcc.send_bytes(fig.to_image('png')) wrapper, also YOUR TURN.)"""
    raise NotImplementedError  # YOUR TURN
