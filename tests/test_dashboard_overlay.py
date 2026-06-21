# test_dashboard_overlay.py
"""Pure trace/axis assertions for app.dashboard.build_overlay_figure.

Exercises the dual-y-axis overlay (series A on the primary axis, series B on a
secondary right-hand axis) with a synthetic comparison dict -- no Dash server,
no DB, no network, no kaleido/to_image.

Run with:  ./.venv/bin/python -m pytest tests/test_dashboard_overlay.py
"""
import os
import sys

import numpy as np
import pandas as pd

# Mirror app/dashboard.py's import bootstrap: src/ for `from compare import ...`
# and the project root so `import app.dashboard` resolves.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import app.dashboard as dashboard  # noqa: E402


def _make_comparison(pct_change=False):
    idx = pd.date_range('2020-01-01', periods=6, freq='MS')
    overlay = pd.DataFrame(
        {'A': np.arange(6.0), 'B': np.arange(6.0) * 100},
        index=idx,
    )
    return {'overlay': overlay, 'pct_change': pct_change}


def test_overlay_has_two_traces():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert len(fig.data) == 2


def test_series_a_on_primary_axis():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert fig.data[0].name == 'A'
    # Primary axis is the default; plotly leaves it unset (None) or 'y'.
    assert fig.data[0].yaxis in (None, 'y')


def test_series_b_on_secondary_axis():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert fig.data[1].name == 'B'
    assert fig.data[1].yaxis == 'y2'


def test_yaxis2_overlays_primary_on_the_right():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert fig.layout.yaxis2.overlaying == 'y'
    assert fig.layout.yaxis2.side == 'right'


def test_empty_overlay_guard_none_returns_zero_traces():
    fig = dashboard.build_overlay_figure(
        {'overlay': None, 'pct_change': False}, 'A', 'B'
    )
    assert len(fig.data) == 0


def test_empty_overlay_guard_empty_frame_returns_zero_traces():
    empty = pd.DataFrame(columns=['A', 'B'])
    fig = dashboard.build_overlay_figure(
        {'overlay': empty, 'pct_change': False}, 'A', 'B'
    )
    assert len(fig.data) == 0


def test_yaxis_has_series_a_title():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert fig.layout.yaxis.title.text == 'A'


def test_yaxis2_has_series_b_title():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert fig.layout.yaxis2.title.text == 'B'


def test_series_a_line_has_explicit_color():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    # Colour set explicitly so it round-trips offline (lazy resolution otherwise).
    assert fig.data[0].line.color is not None
    assert isinstance(fig.data[0].line.color, str)


def test_series_b_line_has_explicit_color():
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert fig.data[1].line.color is not None
    assert isinstance(fig.data[1].line.color, str)


def test_right_axis_colour_matches_series_b_line():
    # The load-bearing cross-equality: series B's line colour == the right
    # axis title font colour == the right axis tickfont colour, so a reader can
    # map the right axis to series B. Assert EQUALITY (not a hex literal) so it
    # survives a template-colorway change.
    fig = dashboard.build_overlay_figure(_make_comparison(), 'A', 'B')
    assert (
        fig.data[1].line.color
        == fig.layout.yaxis2.title.font.color
        == fig.layout.yaxis2.tickfont.color
    )
