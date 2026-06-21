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
firing rows and :func:`summarize_alert` renders a one-line summary.
:func:`build_alerts_payload` is the single PURE entry the UI calls: it maps a
``{indicator -> frame}`` dict plus a list of rules to an ordered list of
per-rule ``{'rule','indicator','fired','n_fired','summary'}`` dicts (a missing
indicator degrades to the empty/"No alerts." case, never a ``KeyError``).
Persisting fired alerts to sqlite is deliberately kept OUT of this module (any
such wrapper must do its own ``import sqlite3`` and stay out of
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
        else:  # 'both' for a level rule = any finite value != threshold.
            # `value != threshold` alone is True for NaN/inf too (they are never
            # "equal"), so guard with np.isfinite to keep the docstring's
            # "NaN values never fire" promise and avoid a spurious NaN-valued row.
            finite = np.isfinite(value.to_numpy(dtype='float64'))
            fired_mask = pd.Series(finite, index=value.index) & (value != rule.threshold)

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


def _describe_verb(rule):
    """Human phrase for what a rule fires on, from kind/direction/threshold.

    Deterministic and pure -- depends only on the rule fields. Examples:
      level/above  -> "above level 5"     (threshold formatted with :g)
      level/below  -> "below level 5"
      level/both   -> "off level 5"
      anomaly/above-> "anomalies above z=3"
      anomaly/below-> "anomalies below z=-3"
      anomaly/both -> "anomalies |z|>3"
    """
    thr = f'{rule.threshold:g}'
    if rule.kind == 'level':
        if rule.direction == 'above':
            return f'above level {thr}'
        if rule.direction == 'below':
            return f'below level {thr}'
        return f'off level {thr}'  # 'both'
    # kind == 'anomaly'
    if rule.direction == 'above':
        return f'anomalies above z={thr}'
    if rule.direction == 'below':
        return f'anomalies below z=-{thr}'
    return f'anomalies |z|>{thr}'  # 'both'


def summarize_alert(rule, fired):
    """One-line human summary of an :func:`evaluate_alert` result.

    ``fired`` is an evaluate_alert frame: columns EXACTLY
    ``['date','value','z_score']``, sorted date-ASCENDING with a 0..n reset
    index, so ``fired.iloc[-1]`` is the latest firing point. PURE -- no I/O, no
    Dash, no sqlite, no network; uses only ``rule`` fields and the rows in hand.

    Empty / None ``fired`` -> the literal ``"No alerts."``. Otherwise returns a
    deterministic line of the shape::

        "<indicator>: <N> point[s] <verb> (latest <YYYY-MM-DD> = <value>[, z=<z>])"

    where N = ``len(fired)`` (pluralizing "point"/"points"), ``<verb>`` comes
    from :func:`_describe_verb` (kind + direction + threshold), and the latest
    date/value come from the last row. For ``kind='anomaly'`` the real latest
    z (``z=<z>``) is appended; for ``kind='level'`` z_score is NaN so it is
    omitted. Floats render with ``:g`` (NaN-tolerant) so the format is total and
    test-assertable. Examples::

        "UNRATE: 3 points above level 5 (latest 2024-06-01 = 5.4)"
        "CPI: 1 point anomalies above z=3 (latest 2024-06-01 = 30.5, z=4.2)"
    """
    if fired is None or len(fired) == 0:
        return 'No alerts.'

    n = len(fired)
    noun = 'point' if n == 1 else 'points'
    verb = _describe_verb(rule)

    last = fired.iloc[-1]
    # Robust to the datetime64 'date' column (and to a plain string/Timestamp).
    latest_date = pd.Timestamp(last['date']).strftime('%Y-%m-%d')
    latest_value = f"{last['value']:g}"  # :g tolerates NaN -> "nan"

    summary = (
        f"{rule.indicator}: {n} {noun} {verb} "
        f"(latest {latest_date} = {latest_value}"
    )
    # Anomaly summaries carry the real latest z; level summaries have NaN z, omit.
    if rule.kind == 'anomaly':
        summary += f", z={last['z_score']:g}"
    summary += ')'
    return summary


def build_alerts_payload(indicator_frames, rules):
    """Map ``{indicator -> frame}`` + a list of rules to ordered per-rule results.

    This is the single PURE entry the UI calls. PURE -- no I/O, no Dash, no
    sqlite, no network, no plotly; it reuses only the in-module
    :func:`evaluate_alert` + :func:`summarize_alert`.

    Parameters
    ----------
    indicator_frames : dict
        ``{indicator_name(str) -> tidy {'date','value'} DataFrame}``. A rule
        whose ``indicator`` is absent (missing key / ``None`` value) is treated
        as having an EMPTY frame, so ``fired`` is the canonical empty result and
        ``summary`` is ``"No alerts."`` -- this NEVER raises ``KeyError``.
    rules : iterable of AlertRule (or ``None``)
        ``None``/empty -> ``[]``.

    Returns
    -------
    list[dict]
        One dict per rule, IN INPUT ORDER::

            {'rule': rule,
             'indicator': rule.indicator,
             'fired': <evaluate_alert(rule, df) DataFrame>,
             'n_fired': int(len(fired)),
             'summary': <summarize_alert(rule, fired) str>}

        ``n_fired == len(fired)`` and the ``fired``/``summary`` values are
        byte-identical to calling :func:`evaluate_alert` /
        :func:`summarize_alert` standalone on the same ``df``.
    """
    payload = []
    if not rules:
        return payload

    for rule in rules:
        # .get(...) so a missing indicator yields None -> evaluate_alert's
        # empty-frame path (never a KeyError); summarize_alert then returns
        # the literal "No alerts.".
        df = indicator_frames.get(rule.indicator) if indicator_frames else None
        fired = evaluate_alert(rule, df)
        summary = summarize_alert(rule, fired)
        payload.append({
            'rule': rule,
            'indicator': rule.indicator,
            'fired': fired,
            'n_fired': int(len(fired)),
            'summary': summary,
        })
    return payload
