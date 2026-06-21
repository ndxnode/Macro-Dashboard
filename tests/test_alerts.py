# test_alerts.py
"""Unit tests for src/alerts.py's PURE alert evaluation (no scipy, no sqlite,
no network).

These exercise AlertRule validation and evaluate_alert directly on synthetic
series. Importing ``alerts`` here also imports ``detect`` (which it reuses), so a
clean collection proves neither pulls in scipy/sqlite at import time.

Run with:  ./.venv/bin/python -m pytest tests/test_alerts.py
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

# Make src/ importable the same way the Dash app (and the other tests) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import alerts  # noqa: E402


def _no_inf(fired):
    """True if neither numeric output column contains +/-inf. (The 'date' column
    is datetime64, so np.isinf is checked only on the value/z_score columns.)"""
    nums = fired[['value', 'z_score']].to_numpy(dtype='float64')
    return not np.isinf(nums).any()


def _make_df(values, start='2020-01-01', freq='MS'):
    """Tidy {'date','value'} DataFrame from a list/array of values."""
    values = np.asarray(values, dtype='float64')
    dates = pd.date_range(start=start, periods=len(values), freq=freq)
    return pd.DataFrame({'date': dates, 'value': values})


# --- (a) anomaly + one local spike: exactly the spike fires ------------------

def test_anomaly_single_local_spike_fires_once():
    rng = np.random.default_rng(0)
    base = 10.0 + rng.normal(scale=0.1, size=40)
    spike_idx = 25
    base[spike_idx] = 30.0  # huge local deviation
    df = _make_df(base)

    rule = alerts.AlertRule(indicator='X', kind='anomaly', threshold=3.0, window=12)
    fired = alerts.evaluate_alert(rule, df)

    assert list(fired.columns) == ['date', 'value', 'z_score']
    assert len(fired) == 1
    # The firing row is the spike date and carries the real (>threshold) z_score.
    assert fired['date'].iloc[0] == df['date'].iloc[spike_idx]
    assert fired['z_score'].iloc[0] > 3.0
    assert _no_inf(fired)


# --- (b) level above/below partition all non-equal points --------------------

def test_level_above_and_below_are_strict_complements():
    values = [1.0, 5.0, 5.0, 9.0, 2.0, 7.0]  # threshold 5.0: below={1,2}, above={9,7}
    df = _make_df(values)

    above = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=5.0, direction='above'),
        df,
    )
    below = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=5.0, direction='below'),
        df,
    )

    assert sorted(above['value'].tolist()) == [7.0, 9.0]
    assert sorted(below['value'].tolist()) == [1.0, 2.0]
    # Together they partition all NON-equal points (the two == 5.0 rows fire neither).
    above_dates = set(above['date'])
    below_dates = set(below['date'])
    assert above_dates.isdisjoint(below_dates)
    non_equal = df[df['value'] != 5.0]
    assert above_dates | below_dates == set(non_equal['date'])
    # level rules carry NaN z_score.
    assert above['z_score'].isna().all()
    # output is sorted ascending by date.
    assert list(above['date']) == sorted(above['date'])


# --- (c) flat/zero-variance: anomaly fires nothing, level still works ---------

def test_flat_series_anomaly_silent_but_level_off_raw_value():
    df = _make_df([5.0] * 20)  # rolling z is NaN (0/0 guard) everywhere

    anom = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='anomaly', threshold=3.0, window=6),
        df,
    )
    assert len(anom) == 0
    assert _no_inf(anom)

    # level 'above' 4.0 on the flat value 5.0 -> every row fires (ignores z).
    lvl_above = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=4.0, direction='above'),
        df,
    )
    assert len(lvl_above) == 20
    # level 'above' 6.0 on the flat value 5.0 -> nothing fires.
    lvl_none = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=6.0, direction='above'),
        df,
    )
    assert len(lvl_none) == 0
    assert list(lvl_none.columns) == ['date', 'value', 'z_score']


# --- (d) empty and short input: empty frame, no crash, no inf ----------------

def test_empty_and_short_inputs_return_empty_frame():
    empty = pd.DataFrame(columns=['date', 'value'])
    for kind in ('anomaly', 'level'):
        rule = alerts.AlertRule(indicator='X', kind=kind, threshold=1.0)
        out = alerts.evaluate_alert(rule, empty)
        assert isinstance(out, pd.DataFrame)
        assert list(out.columns) == ['date', 'value', 'z_score']
        assert len(out) == 0
        assert _no_inf(out)

    # 1- and 2-row anomaly inputs: not enough history -> empty, no crash, no inf.
    for n in (1, 2):
        short = _make_df(list(np.arange(1.0, n + 1.0)))
        out = alerts.evaluate_alert(
            alerts.AlertRule(indicator='X', kind='anomaly', threshold=3.0, window=12),
            short,
        )
        assert list(out.columns) == ['date', 'value', 'z_score']
        assert _no_inf(out)


def test_missing_columns_and_all_nan_return_empty():
    # Missing 'value' column.
    bad = pd.DataFrame({'date': pd.date_range('2020-01-01', periods=3, freq='MS')})
    out = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=0.0), bad
    )
    assert len(out) == 0
    assert list(out.columns) == ['date', 'value', 'z_score']

    # All-NaN values.
    nan_df = _make_df([np.nan, np.nan, np.nan])
    out2 = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=0.0, direction='above'),
        nan_df,
    )
    assert len(out2) == 0


# --- (e) invalid rule fields raise ValueError --------------------------------

def test_invalid_kind_and_direction_raise():
    with pytest.raises(ValueError):
        alerts.AlertRule(indicator='X', kind='bogus')
    with pytest.raises(ValueError):
        alerts.AlertRule(indicator='X', direction='sideways')


# --- (f) level 'both' must not fire on NaN/inf values ------------------------

def test_level_both_skips_non_finite_values():
    # The 'both' level rule is `value != threshold`; a naive form fires on NaN
    # and inf (which are never "equal") and would emit a spurious NaN-valued row,
    # breaking the docstring's "NaN values never fire" promise. Only the finite
    # non-equal point (7.0) should fire here.
    df = _make_df([5.0, np.nan, np.inf, 7.0, 5.0])
    fired = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='level', threshold=5.0, direction='both'),
        df,
    )
    assert fired['value'].tolist() == [7.0]
    assert not fired['value'].isna().any()
    assert _no_inf(fired)


# --- extra: anomaly direction 'above' vs 'below' on a real spike -------------

def test_anomaly_direction_above_catches_positive_spike_below_does_not():
    rng = np.random.default_rng(1)
    base = 10.0 + rng.normal(scale=0.1, size=40)
    base[25] = 30.0  # positive spike
    df = _make_df(base)

    above = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='anomaly', threshold=3.0,
                         window=12, direction='above'),
        df,
    )
    below = alerts.evaluate_alert(
        alerts.AlertRule(indicator='X', kind='anomaly', threshold=3.0,
                         window=12, direction='below'),
        df,
    )
    assert len(above) == 1
    assert above['z_score'].iloc[0] > 3.0
    # A positive spike is not a 'below' (negative-z) event.
    assert len(below) == 0
