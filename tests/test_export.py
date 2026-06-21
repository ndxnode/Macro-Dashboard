# test_export.py
"""Unit tests for src/export.py's PURE export helpers (no scipy, no sqlite, no
network, no kaleido).

These exercise build_csv_payload / build_export_figure / clip_to_last_years
directly on synthetic date-indexed overlay frames. plotly is imported lazily
inside build_export_figure, so a clean collection also proves the CSV/clip core
imports without plotly. No ``fig.to_image``/``write_image`` is ever called (that
needs kaleido, absent here).

Run with:  ./.venv/bin/python -m pytest tests/test_export.py
"""
import io
import os
import sys

import numpy as np
import pandas as pd
import pytest

# Make src/ importable the same way the Dash app (and the other tests) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import export  # noqa: E402


def _make_overlay(n=36, start='2020-01-01', freq='MS'):
    """A synthetic date-indexed overlay frame: two series columns over `n`
    monthly periods (the shape compare.build_comparison returns)."""
    idx = pd.date_range(start=start, periods=n, freq=freq)
    return pd.DataFrame(
        {
            'series_a': np.arange(n, dtype='float64'),
            'series_b': np.arange(n, dtype='float64') * 2.0 + 1.0,
        },
        index=idx,
    )


# ---- (a) CSV payload round-trips --------------------------------------------

def test_csv_payload_round_trips():
    frame = _make_overlay(n=12)
    payload = export.build_csv_payload(frame)
    assert isinstance(payload, str) and payload != ''

    back = pd.read_csv(io.StringIO(payload))
    # The index column is named via index_label (default 'date') + the columns.
    assert list(back.columns) == ['date', 'series_a', 'series_b']
    # Values reconstruct.
    np.testing.assert_allclose(back['series_a'].to_numpy(), frame['series_a'].to_numpy())
    np.testing.assert_allclose(back['series_b'].to_numpy(), frame['series_b'].to_numpy())
    # The named date column round-trips to the original index dates.
    assert list(pd.to_datetime(back['date'])) == list(frame.index)


def test_csv_payload_custom_index_label_and_no_mutation():
    frame = _make_overlay(n=6)
    original_index_name = frame.index.name  # None
    payload = export.build_csv_payload(frame, index_label='month')
    back = pd.read_csv(io.StringIO(payload))
    assert back.columns[0] == 'month'
    # Caller's frame is NOT mutated (index name unchanged).
    assert frame.index.name == original_index_name


# ---- (b) build_export_figure -------------------------------------------------

def test_export_figure_one_trace_per_column_named():
    import plotly.graph_objects as go

    frame = _make_overlay(n=10)
    fig = export.build_export_figure(frame)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == len(frame.columns)
    assert [tr.name for tr in fig.data] == [str(c) for c in frame.columns]


def test_export_figure_title_applied_when_passed():
    frame = _make_overlay(n=4)
    fig_no_title = export.build_export_figure(frame)
    assert fig_no_title.layout.title.text is None

    fig = export.build_export_figure(frame, title='Overlay')
    assert fig.layout.title.text == 'Overlay'


# ---- (c) clip_to_last_years --------------------------------------------------

def test_clip_keeps_only_rows_within_n_years():
    frame = _make_overlay(n=120)  # 10 years of monthly data
    clipped = export.clip_to_last_years(frame, 1)
    cutoff = frame.index.max() - pd.DateOffset(years=1)
    assert (clipped.index >= cutoff).all()
    assert len(clipped) < len(frame)
    # 1 year of monthly data (inclusive of cutoff month) -> 13 rows.
    assert len(clipped) == 13


def test_clip_none_and_nonpositive_years_are_identity():
    frame = _make_overlay(n=24)
    assert len(export.clip_to_last_years(frame, None)) == len(frame)
    assert len(export.clip_to_last_years(frame, 0)) == len(frame)
    assert len(export.clip_to_last_years(frame, -5)) == len(frame)


def test_clip_empty_frame_returned_as_is():
    empty = pd.DataFrame()
    out = export.clip_to_last_years(empty, 5)
    assert out.empty


def test_clip_non_datetime_index_returned_as_is():
    frame = pd.DataFrame({'series_a': [1.0, 2.0, 3.0]}, index=[0, 1, 2])
    out = export.clip_to_last_years(frame, 5)
    # Returned unchanged, no raise.
    assert len(out) == 3
    assert list(out['series_a']) == [1.0, 2.0, 3.0]


def test_clip_fractional_float_years_does_not_raise():
    # pd.DateOffset(years=1.5) raises a dateutil ValueError ("Non-integer years
    # ... not currently supported"). The helper must degrade gracefully to a
    # whole-year window (int(1.5) -> 1) instead of crashing, like the integer
    # preset path -- 10 years of monthly data clipped to ~1Y -> 13 rows.
    frame = _make_overlay(n=120)
    clipped = export.clip_to_last_years(frame, 1.5)
    assert len(clipped) == 13
    # And an integer-valued float matches the int preset exactly.
    assert len(export.clip_to_last_years(frame, 1.0)) == len(
        export.clip_to_last_years(frame, 1)
    )


# ---- (d) empty / None frame: no raises --------------------------------------

def test_empty_and_none_frames_do_not_raise():
    import plotly.graph_objects as go

    empty = pd.DataFrame()
    assert export.build_csv_payload(empty) == ''
    assert export.build_csv_payload(None) == ''

    fig_empty = export.build_export_figure(empty)
    fig_none = export.build_export_figure(None)
    assert isinstance(fig_empty, go.Figure) and len(fig_empty.data) == 0
    assert isinstance(fig_none, go.Figure) and len(fig_none.data) == 0


# ---- (e) download_filename ---------------------------------------------------

def test_download_filename_two_series_with_preset():
    assert export.download_filename('GDP', 'CPI', '5Y') == 'GDP_vs_CPI_5Y.csv'


def test_download_filename_single_series_no_preset():
    # name_b None -> just the one part; no preset token appended.
    assert export.download_filename('GDP') == 'GDP.csv'
    assert export.download_filename('GDP', None, None) == 'GDP.csv'


def test_download_filename_sanitizes_unsafe_chars():
    # Spaces / slashes / other unsafe chars become underscores; the two series
    # join with the 'vs' connector.
    assert export.download_filename('CPI All Items', 'A/B') == 'CPI_All_Items_vs_A_B.csv'


def test_download_filename_falls_back_to_export_when_no_usable_parts():
    # Empty / None / whitespace-only parts are dropped -> 'export' stem.
    assert export.download_filename(None, None) == 'export.csv'
    assert export.download_filename('', '   ') == 'export.csv'
    # A truthy preset still cannot rescue a missing stem on its own? It is a part,
    # so it DOES contribute -- but with no names AND no preset, fall back.
    assert export.download_filename(None) == 'export.csv'


def test_download_filename_custom_ext():
    assert export.download_filename('GDP', ext='txt') == 'GDP.txt'


def test_download_filename_preset_only_appended_when_truthy():
    # An empty / None preset is NOT appended.
    assert export.download_filename('GDP', 'CPI', '') == 'GDP_vs_CPI.csv'
    assert export.download_filename('GDP', 'CPI', None) == 'GDP_vs_CPI.csv'


# ---- (f) preset_years --------------------------------------------------------

def test_preset_years_known_tokens():
    assert export.preset_years('1Y') == 1
    assert export.preset_years('5Y') == 5
    assert export.preset_years('10Y') == 10


def test_preset_years_max_none_and_unknown_are_identity():
    assert export.preset_years('Max') is None
    assert export.preset_years(None) is None
    assert export.preset_years('bogus') is None


def test_preset_years_feeds_clip_to_last_years_1y_window():
    # Integration-style: '1Y' -> 1 -> the same 1-year window clip tests cover.
    frame = _make_overlay(n=120)  # 10 years monthly
    clipped = export.clip_to_last_years(frame, export.preset_years('1Y'))
    assert len(clipped) == 13
    # 'Max' -> None -> identity (no clip).
    assert len(export.clip_to_last_years(frame, export.preset_years('Max'))) == len(frame)
