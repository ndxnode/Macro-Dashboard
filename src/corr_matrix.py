# corr_matrix.py
"""Pairwise correlation matrix across all macro indicators.

This is a pure pandas/numpy helper with no database, network, or plotting
dependency, so it can be unit-tested against synthetic data (there is likely
no FRED_API_KEY in this environment, and kaleido/scipy are absent). Nothing
here imports sqlite3/Dash/plotly/kaleido at module top -- this mirrors the
import purity of ``src/compare.py`` and ``src/signals.py`` so the module can be
exercised offline.

Each indicator is expected as a tidy DataFrame with at least 'date' and 'value'
columns -- the same ``{indicator_name -> tidy {'date','value'} frame}`` dict
shape ``alerts.build_alerts_payload`` already consumes (and the same per-series
shape ``compare.py``/``signals.py`` use).

The flagship helper, ``correlation_matrix``, takes that dict and returns a
SYMMETRIC, sorted, square DataFrame of pairwise Pearson correlations -- a
ready-to-render heatmap source (see the px.imshow YOUR TURN seam below).
"""
import pandas as pd
import numpy as np


def _to_dated_series(df, name='value'):
    """Normalize a tidy {'date','value'} DataFrame into a named, date-indexed
    numeric Series sorted by date with duplicate dates dropped (last wins).

    A local copy of ``compare._to_dated_series`` so this module stays
    self-contained and import-pure (no cross-module import); identical body.
    """
    if df is None or df.empty or 'date' not in df.columns or 'value' not in df.columns:
        return pd.Series(dtype='float64', name=name)

    s = df[['date', 'value']].copy()
    s['date'] = pd.to_datetime(s['date'], errors='coerce')
    s['value'] = pd.to_numeric(s['value'], errors='coerce')
    s = s.dropna(subset=['date'])
    s = s.sort_values('date').drop_duplicates(subset='date', keep='last')
    return pd.Series(s['value'].values, index=s['date'].values, name=name)


def correlation_matrix(indicator_frames, method='pearson', min_overlap=3):
    """Build a symmetric pairwise correlation matrix across all indicators.

    ``indicator_frames`` is a ``{indicator_name -> tidy {'date','value'} frame}``
    dict (the same shape ``alerts.build_alerts_payload`` consumes). Each frame is
    normalized through ``_to_dated_series`` (date-indexed, sorted, deduped,
    coerced) and the series are aligned on their dates; the result is a square
    DataFrame whose index == columns == ``sorted(indicator_frames)`` holding the
    pairwise ``method`` (default 'pearson') correlations.

    Returns a SYMMETRIC DataFrame:
      * a perfectly-correlated pair -> +1.0, anti-correlated -> -1.0, an
        independent pair -> ~0 (sign correct);
      * the diagonal is EXACTLY 1.0 (a series is trivially self-correlated);
      * a pair sharing fewer than ``min_overlap`` dates -> NaN off-diagonal;
      * an empty dict -> an empty 0x0 DataFrame; a single indicator -> 1x1
        ``[[1.0]]``.
    It NEVER raises on empty / single / all-NaN / non-overlapping input.

    THREE LOAD-BEARING DECISIONS (venv-confirmed):
      1. EMPTY-DICT GUARD -- ``pd.concat([], axis=1)`` raises
         ``ValueError: No objects to concatenate``, so an empty dict is guarded
         BEFORE the concat and returns an empty 0x0 frame.
      2. OUTER JOIN -- aligning the series with ``join='outer'`` keeps every
         pair's overlap pairwise-complete; ``DataFrame.corr`` is itself
         pairwise-complete, so a single sparse indicator does NOT shrink the
         whole matrix (an inner join would gut every pair down to the dates
         common to ALL indicators).
      3. DIAGONAL FORCED TO 1.0 -- ``DataFrame.corr(min_periods=...)`` leaves
         the diagonal NaN for an all-NaN / zero-variance (std=0) /
         fewer-than-``min_overlap``-own-points series. ``np.fill_diagonal`` on a
         ``.to_numpy(copy=True)`` COPY forces the 'diagonal == 1.0' contract
         unconditionally. The ``reindex`` BEFORE the fill keeps the matrix a full
         sorted square even if ``.corr`` drops an all-NaN column.

    ``method`` and ``min_overlap`` are pass-throughs to ``DataFrame.corr``
    (``min_periods=min_overlap`` is exactly the NaN-on-thin-overlap guard).
    """
    names = sorted(indicator_frames.keys())

    # DECISION 1: guard the empty dict -- pd.concat([]) would raise.
    if not names:
        return pd.DataFrame()

    series = [_to_dated_series(indicator_frames[n], name=n) for n in names]
    # DECISION 2: outer join keeps each pair's overlap pairwise-complete.
    wide = pd.concat(series, axis=1, join='outer')
    wide.columns = names  # pin column order/labels to the sorted names

    cm = wide.corr(method=method, min_periods=min_overlap)
    # Force the exact sorted-square shape even if .corr dropped an all-NaN column.
    cm = cm.reindex(index=names, columns=names)

    # DECISION 3: force diagonal == 1.0 on a copy (corr leaves all-NaN /
    # zero-variance / <min_overlap diagonals NaN otherwise).
    arr = cm.to_numpy(dtype='float64', copy=True)
    np.fill_diagonal(arr, 1.0)
    return pd.DataFrame(arr, index=names, columns=names)


# YOUR TURN: a thin Dash panel rendering the matrix as a heatmap. Once the app
# builds the ``{indicator -> tidy frame}`` dict (the same one
# ``alerts.build_alerts_payload`` consumes), a diverging red-blue heatmap is
# essentially one call:
#     import plotly.express as px
#     fig = px.imshow(
#         correlation_matrix(frames),
#         zmin=-1, zmax=1,                  # fix the colour range to [-1, 1]
#         color_continuous_scale='RdBu',    # diverging, centred at 0
#         text_auto='.2f',                  # annotate each cell
#     )
# The pure, tested part is ``correlation_matrix`` above; the plotting/wiring
# stays here as a later increment so this module is import-pure (plotly must not
# enter sys.modules via a plain ``import corr_matrix``).
