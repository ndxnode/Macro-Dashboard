# signals.py
"""Recession-signal helpers (the Sahm rule + a generic threshold-cross flag).

These are pure pandas/numpy helpers with no database, network, or plotting
dependency, so they can be unit-tested against synthetic data (there is likely
no FRED_API_KEY in this environment, and kaleido/scipy are absent). Nothing here
imports sqlite3/Dash/plotly/kaleido at module top -- this mirrors the import
purity of ``src/compare.py`` so the module can be exercised offline.

Each series is expected as a tidy DataFrame with at least 'date' and 'value'
columns -- the same shape ``app.dashboard.get_data_for_indicator_graph``, the
``macro_data`` table, and ``compare.py``/``alerts.py`` already use.

The flagship helper is the Sahm rule: it fires when the 3-month moving average
of the Unemployment Rate (FRED ``UNRATE``; the 'Unemployment Rate' indicator
name etl.py / the DB / the dropdowns use) rises >= 0.5 percentage points above
its trailing 12-month minimum -- a well-known real-time recession signal.
"""
import pandas as pd
import numpy as np


# The canonical Sahm trigger level (percentage points). Exposed as a module
# constant so the YOUR TURN Dash panel below (a 0.5 threshold hline) and any
# future configurable rule can reference one source of truth.
SAHM_THRESHOLD = 0.5


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


def sahm_rule(unrate_frame, window=12):
    """Compute the Sahm recession indicator from an Unemployment Rate series.

    The 3-month moving average of the rate (``sma3``) is compared to its
    trailing ``window``-month minimum (``trough``); the gap is the Sahm value.
    The classic rule fires when that gap reaches ``SAHM_THRESHOLD`` (0.5 pts).

    Returns a tidy DataFrame with columns EXACTLY ['date','sahm','triggered']
    (date ascending, index reset). 'triggered' is a clean bool dtype.

    Both ``min_periods`` are LOAD-BEARING: ``rolling(3, min_periods=3).mean()``
    and ``rolling(window, min_periods=window).min()`` force the first 1-2 / the
    first ``window-1`` rows to NaN instead of emitting a partial-window result
    that could falsely flag (the same NaN-not-flag discipline as
    ``detect.rolling_zscore``). NaN sahm -> triggered False. Any pathological
    +/-inf in sahm is scrubbed to NaN before the frame is built.

    Empty / None / missing-cols / all-NaN input -> ``_to_dated_series`` yields an
    empty Series -> a canonical EMPTY frame with the same three columns and 0
    rows; this NEVER raises.
    """
    s = _to_dated_series(unrate_frame)

    if s.empty:
        return pd.DataFrame({
            'date': pd.Series([], dtype='datetime64[ns]'),
            'sahm': pd.Series([], dtype='float64'),
            'triggered': pd.Series([], dtype='bool'),
        })

    sma3 = s.rolling(window=3, min_periods=3).mean()              # 3-month moving avg
    trough = sma3.rolling(window=window, min_periods=window).min()  # trailing window-min
    sahm = sma3 - trough
    # Defensive inf-scrub (mirrors detect): the venv run showed no inf arises,
    # but a pathological input must not be able to leak +/-inf into the frame.
    sahm = sahm.replace([np.inf, -np.inf], np.nan)
    triggered = (sahm >= SAHM_THRESHOLD).where(sahm.notna(), other=False)

    return pd.DataFrame({
        'date': s.index,
        'sahm': sahm.values,
        'triggered': triggered.values,
    }).reset_index(drop=True)


def threshold_cross_flag(frame, level, direction='above'):
    """Flag rows of a generic tidy {'date','value'} series that cross ``level``.

    Returns a tidy DataFrame with columns EXACTLY ['date','value','flag'] (date
    ascending, index reset). 'flag' is a clean bool dtype.

    ``direction='above'`` flags ``value > level``; ``'below'`` flags
    ``value < level``. Anything else raises ``ValueError`` -- this mirrors
    ``AlertRule.__post_init__``'s validate-direction-with-ValueError convention
    (alerts.py) rather than silently degrading. NaN / +inf / -inf NEVER flag
    (the ``np.isfinite`` mask zeroes them in either direction).

    Empty / None / missing-cols -> a canonical EMPTY frame with the same three
    columns and 0 rows.
    """
    if direction not in ('above', 'below'):
        raise ValueError(
            f"direction must be 'above' or 'below', got {direction!r}"
        )

    s = _to_dated_series(frame)

    if s.empty:
        return pd.DataFrame({
            'date': pd.Series([], dtype='datetime64[ns]'),
            'value': pd.Series([], dtype='float64'),
            'flag': pd.Series([], dtype='bool'),
        })

    values = s.values
    finite = np.isfinite(values)
    if direction == 'above':
        flag = (values > level) & finite
    else:  # 'below' (validated above)
        flag = (values < level) & finite

    return pd.DataFrame({
        'date': s.index,
        'value': values,
        'flag': flag,
    }).reset_index(drop=True)


# YOUR TURN: a yield-curve (10y-2y) inversion flag as a v2 recession signal.
# It needs a rates-spread series (10-year minus 2-year Treasury) that is not yet
# in the DB. Once such a {'date','value'} spread frame exists,
# ``threshold_cross_flag(spread_frame, 0, 'below')`` is the ready building block
# -- an inversion is simply the spread crossing below zero.

# YOUR TURN: a thin Dash panel for the Sahm series -- a ``px.line`` of
# ``sahm_rule(unrate_frame)['sahm']`` with a horizontal threshold line at
# ``SAHM_THRESHOLD`` (0.5) and shaded vertical spans over the 'triggered' rows.
# The pure, tested part is ``sahm_rule`` + ``threshold_cross_flag`` above; the
# plotting/wiring stays here as a later increment so this module is import-pure.
