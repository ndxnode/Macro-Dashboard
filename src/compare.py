# compare.py
"""Correlation / comparison helpers for overlaying two indicator series.

These are pure pandas/numpy helpers with no database or network dependency, so
they can be unit-tested against synthetic data (there is likely no FRED_API_KEY
in this environment). The Dash app can import `build_comparison` to get an
aligned overlay, a %-change transform, and a rolling correlation between two
indicator series.

Each series is expected as a tidy DataFrame with at least 'date' and 'value'
columns -- the same shape returned by
``app.dashboard.get_data_for_indicator_graph`` and the ``macro_data`` table.
"""
import pandas as pd
import numpy as np


def _to_dated_series(df, name):
    """Normalize a tidy {'date','value'} DataFrame into a named, date-indexed
    numeric Series sorted by date with duplicate dates dropped (last wins)."""
    if df is None or df.empty or 'date' not in df.columns or 'value' not in df.columns:
        return pd.Series(dtype='float64', name=name)

    s = df[['date', 'value']].copy()
    s['date'] = pd.to_datetime(s['date'], errors='coerce')
    s['value'] = pd.to_numeric(s['value'], errors='coerce')
    s = s.dropna(subset=['date'])
    s = s.sort_values('date').drop_duplicates(subset='date', keep='last')
    return pd.Series(s['value'].values, index=s['date'].values, name=name)


def align_series(df_a, df_b, name_a='series_a', name_b='series_b', how='inner'):
    """Align two indicator series onto a shared date index.

    Returns a DataFrame indexed by date with one column per series. With the
    default ``how='inner'`` only dates present in BOTH series are kept, which is
    what an overlay / correlation needs. Use ``how='outer'`` to keep every date
    (gaps become NaN) -- useful when the two indicators report on different
    cadences and you want to forward-fill before plotting.
    """
    sa = _to_dated_series(df_a, name_a)
    sb = _to_dated_series(df_b, name_b)
    aligned = pd.concat([sa, sb], axis=1, join=how)
    aligned.index.name = 'date'
    return aligned.sort_index()


def pct_change_transform(aligned, periods=1):
    """Return a percent-change (period-over-period) version of an aligned frame.

    Each column is converted to its percentage change so two series on very
    different scales (e.g. a price index vs. a rate) can share one y-axis.
    Result is expressed in percent (multiplied by 100). The first ``periods``
    rows are NaN by construction and are dropped.

    A prior value of exactly 0 makes ``pct_change`` produce ``+/-inf`` (a real
    case for macro data: a rate pinned at the zero lower bound, a net-change
    series, etc.). Those infinities are treated as missing (NaN) so they are
    excluded from downstream correlations instead of silently poisoning every
    Pearson r to NaN -- ``inf`` survives ``dropna``/``.corr`` and would wipe out
    an otherwise computable correlation.
    """
    if aligned is None or aligned.empty:
        return aligned.copy() if aligned is not None else aligned
    pct = aligned.pct_change(periods=periods) * 100.0
    pct = pct.replace([np.inf, -np.inf], np.nan)
    return pct.dropna(how='all')


def rolling_correlation(aligned, window=12, min_periods=None):
    """Compute a rolling Pearson correlation between the two columns of an
    aligned frame.

    Returns a Series indexed by date. ``window`` is in observations (rows), so
    its meaning depends on the data cadence (e.g. 12 monthly points = 1 year).
    Constant windows (zero variance) yield NaN, which callers can drop or plot
    as a gap.
    """
    if aligned is None or aligned.shape[1] < 2:
        return pd.Series(dtype='float64', name='rolling_corr')
    if min_periods is None:
        min_periods = window
    col_a, col_b = aligned.columns[0], aligned.columns[1]
    corr = (
        aligned[col_a]
        .rolling(window=window, min_periods=min_periods)
        .corr(aligned[col_b])
    )
    corr.name = 'rolling_corr'
    return corr


def build_comparison(df_a, df_b, name_a='series_a', name_b='series_b',
                     window=12, pct_change=False, periods=1):
    """Bundle the pieces a comparison/overlay view needs.

    Aligns the two series, optionally converts them to %-change, computes the
    overall (full-sample) Pearson correlation and a rolling correlation, and
    reports how many overlapping observations were found.

    Returns a dict with keys:
      - ``overlay``      : aligned DataFrame (two columns, ready for a dual
                           y-axis Plotly figure)
      - ``rolling_corr`` : rolling-correlation Series
      - ``overall_corr`` : float full-sample Pearson r (NaN if undefined)
      - ``n_overlap``    : int number of shared, non-NaN observations
      - ``window``       : the rolling window used
      - ``pct_change``   : whether the overlay is in %-change space
    """
    aligned = align_series(df_a, df_b, name_a=name_a, name_b=name_b, how='inner')
    overlay = pct_change_transform(aligned, periods=periods) if pct_change else aligned

    # Effective window cannot exceed the number of rows we actually have.
    eff_window = max(2, min(window, len(overlay))) if len(overlay) else window
    roll = rolling_correlation(overlay, window=eff_window)

    paired = overlay.dropna()
    if paired.shape[0] >= 2 and paired.shape[1] >= 2:
        overall = float(paired.iloc[:, 0].corr(paired.iloc[:, 1]))
    else:
        overall = float('nan')

    return {
        'overlay': overlay,
        'rolling_corr': roll,
        'overall_corr': overall,
        'n_overlap': int(paired.shape[0]),
        'window': eff_window,
        'pct_change': bool(pct_change),
    }
