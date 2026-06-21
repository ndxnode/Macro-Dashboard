# alerts.py
"""User-defined alerts over a single indicator series.

This is a PURE numpy/pandas module with no database or network dependency (no
``import sqlite3`` at module top, no FRED pull), so it can be unit-tested against
synthetic data -- the same discipline as compare.py / detect.py. It consumes the
same tidy ``{'date','value'}`` DataFrame shape used elsewhere (the ``macro_data``
table / ``app.dashboard.get_data_for_indicator_graph``) and reuses the existing
rolling-z anomaly core via :func:`detect.detect_anomalies`, so the "anomaly"
alert kind shares exactly one scoring path with the rest of the app.

An :class:`AlertRule` describes what to watch; :func:`evaluate_alert` returns the
firing rows. Persisting fired alerts to sqlite is deliberately kept OUT of this
module (any such wrapper must do its own ``import sqlite3`` and stay out of
``evaluate_alert``) so tests never open a DB.
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd

import detect


# A plain dataclass (not a bare dict) is the design choice here: callers could
# pass a dict, but the dataclass gives the kind/direction validation a home in
# __post_init__ and keeps the field order/defaults explicit for a later UI hook.
@dataclass
class AlertRule:
    """A simple alert over one indicator's {'date','value'} series.

    Fields
    ------
    indicator : str
        Name of the indicator the rule watches (informational; the series is
        passed separately to :func:`evaluate_alert`).
    kind : str
        ``'anomaly'`` (rolling-z outlier via detect) or ``'level'`` (raw value
        vs ``threshold``). One of ``{'anomaly','level'}``.
    threshold : float
        z-threshold when ``kind='anomaly'``; raw level when ``kind='level'``.
    window : int
        Rolling window passed to detect (used by the anomaly kind only).
    direction : str
        ``'above'``, ``'below'`` or ``'both'``.
    """

    indicator: str
    kind: str = 'anomaly'        # one of {'anomaly','level'}
    threshold: float = 3.0       # z-threshold when kind='anomaly', raw level when 'level'
    window: int = 12             # rolling window passed to detect (anomaly kind only)
    direction: str = 'both'      # one of {'above','below','both'}

    def __post_init__(self):
        if self.kind not in {'anomaly', 'level'}:
            raise ValueError(
                f"kind must be one of {{'anomaly','level'}}, got {self.kind!r}"
            )
        if self.direction not in {'above', 'below', 'both'}:
            raise ValueError(
                "direction must be one of {'above','below','both'}, "
                f"got {self.direction!r}"
            )


# Output schema for every evaluate_alert return (firing rows AND the empty case),
# so callers can len()/iterate without special-casing.
_OUTPUT_COLUMNS = ['date', 'value', 'z_score']


def _empty_result():
    """Canonical empty firing frame: the three output columns, no rows."""
    return pd.DataFrame({
        'date': pd.Series([], dtype='datetime64[ns]'),
        'value': pd.Series([], dtype='float64'),
        'z_score': pd.Series([], dtype='float64'),
    })


def _to_dated_series(df):
    """Normalize a tidy {'date','value'} DataFrame into a date-indexed, date-
    sorted, dup-dropped numeric Series -- the same contract as
    compare._to_dated_series (pd.to_datetime / pd.to_numeric both
    ``errors='coerce'``, drop rows with a NaT date)."""
    if df is None or df.empty or 'date' not in df.columns or 'value' not in df.columns:
        return pd.Series(dtype='float64')

    s = df[['date', 'value']].copy()
    s['date'] = pd.to_datetime(s['date'], errors='coerce')
    s['value'] = pd.to_numeric(s['value'], errors='coerce')
    s = s.dropna(subset=['date'])
    s = s.sort_values('date').drop_duplicates(subset='date', keep='last')
    return pd.Series(s['value'].values, index=s['date'].values)


def evaluate_alert(rule, df):
    """Evaluate an :class:`AlertRule` against a tidy {'date','value'} frame.

    Returns a tidy DataFrame with columns EXACTLY ``['date','value','z_score']``,
    one row per firing point, SORTED by date ascending, with a reset (0..n)
    index so ``date`` is a column. Empty / missing-cols / all-NaN input yields
    the canonical empty frame (same columns/dtypes) rather than raising. No
    ``+/-inf`` ever appears in the output.

    kind='anomaly'
        Scores via :func:`detect.detect_anomalies` (window=rule.window,
        z_threshold=rule.threshold), then selects firing rows by direction:
          - 'both'  -> ``is_outlier`` (abs-based in detect);
          - 'above' -> ``z_score >  threshold`` (NaN compares False);
          - 'below' -> ``z_score < -threshold``.
        Firing rows carry the real z_score.

    kind='level'
        Fires on the RAW value vs ``threshold``:
          - 'above' -> ``value >  threshold``;
          - 'below' -> ``value <  threshold``;
          - 'both'  -> ``value != threshold`` (the niche case: any non-equal
            point; defined, not surprising).
        NaN values never fire; the z_score column is NaN for level rules.
    """
    series = _to_dated_series(df)
    if series.empty or series.isna().all():
        return _empty_result()

    if rule.kind == 'anomaly':
        scored = detect.detect_anomalies(
            series.values,
            window=rule.window,
            z_threshold=rule.threshold,
        )
        # Align detect's positional output back onto the real dates.
        z = pd.Series(scored['z_score'].values, index=series.index)
        value = pd.Series(scored['value'].values, index=series.index)

        if rule.direction == 'above':
            fired_mask = z > rule.threshold          # NaN compares False
        elif rule.direction == 'below':
            fired_mask = z < -rule.threshold
        else:  # 'both' -- reuse detect's abs-based is_outlier directly
            fired_mask = pd.Series(scored['is_outlier'].values, index=series.index)

        out = pd.DataFrame({'date': series.index, 'value': value.values,
                            'z_score': z.values})
        out = out[fired_mask.values]
    else:  # kind == 'level': raw value vs threshold; z_score is NaN.
        value = series
        if rule.direction == 'above':
            fired_mask = value > rule.threshold       # NaN compares False
        elif rule.direction == 'below':
            fired_mask = value < rule.threshold
        else:  # 'both' for a level rule = any value != threshold (NaN never fires)
            fired_mask = value != rule.threshold

        out = pd.DataFrame({
            'date': series.index,
            'value': value.values,
            'z_score': np.nan,
        })
        out = out[fired_mask.values]

    # Sort ascending by date (do not silently reverse), scrub any inf, reset index
    # so 'date' is a column. Guard inf even on the level path.
    out = out.sort_values('date')
    out['z_score'] = out['z_score'].replace([np.inf, -np.inf], np.nan)
    out['value'] = out['value'].replace([np.inf, -np.inf], np.nan)
    out = out[_OUTPUT_COLUMNS].reset_index(drop=True)
    return out


def summarize_alert(rule, fired):
    """One-line human summary of an evaluate_alert result, e.g.
    "UNRATE: 3 points above level 5.0 (latest 2024-06-01 = 5.4)".
    # YOUR TURN: build the f-string from rule.kind/direction/threshold and the
    # last row of `fired`; return "No alerts." when fired is empty. Keep it pure
    # (no I/O). A test for this is intentionally NOT included yet."""
    raise NotImplementedError  # YOUR TURN
