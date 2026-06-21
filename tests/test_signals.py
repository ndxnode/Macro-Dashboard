# test_signals.py
"""Unit tests for src/signals.py's PURE Sahm-rule + threshold-cross helpers.

These exercise the recession-signal core directly on synthetic series -- no
sqlite, no Dash, no network, no kaleido. Importing ``signals`` here also proves
the module stays import-pure (only pandas/numpy enter).

Run with:  ./.venv/bin/python -m pytest tests/test_signals.py
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

# Make src/ importable the same way the Dash app (and test_compare) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import signals  # noqa: E402


def _frame(values, start='2018-01-01', freq='MS'):
    dates = pd.date_range(start=start, periods=len(values), freq=freq)
    return pd.DataFrame({'date': dates, 'value': values})


# --- sahm_rule -------------------------------------------------------------

def test_rising_unrate_fires_at_expected_date():
    # 15 months flat at 3.5, then a climb. Fixed date_range -> deterministic.
    vals = [3.5] * 15 + [3.6, 3.8, 4.1, 4.5, 5.0, 5.6, 6.3, 7.0, 8.0]
    assert len(vals) == 24
    out = signals.sahm_rule(_frame(vals))

    assert list(out.columns) == ['date', 'sahm', 'triggered']
    assert len(out) == 24
    assert out['triggered'].dtype == bool

    fired = out[out['triggered']]
    assert not fired.empty
    first = fired.iloc[0]
    # sahm crosses 0.5 (=0.633) at this exact row.
    assert pd.Timestamp(first['date']) == pd.Timestamp('2019-07-01')
    assert first['sahm'] >= signals.SAHM_THRESHOLD

    # Every row before the trailing 12-month trough fills is NaN sahm / False.
    early = out.iloc[:11]
    assert early['sahm'].isna().all()
    assert not early['triggered'].any()


def test_flat_series_never_triggers():
    out = signals.sahm_rule(_frame([4.0] * 24))
    assert not out['triggered'].any()
    # The max non-NaN sahm is exactly 0.0 (sma3 == its own trailing min).
    assert out['sahm'].max() == 0.0


def test_declining_series_never_triggers():
    out = signals.sahm_rule(_frame(list(np.linspace(8, 3, 24))))
    assert not out['triggered'].any()
    # Trough tracks the falling sma3, so sahm stays pinned at 0.0.
    assert np.nanmax(out['sahm'].to_numpy(dtype='float64')) == 0.0


def test_short_series_no_trough_no_trigger_no_raise():
    out = signals.sahm_rule(_frame([3.0, 3.2, 3.5, 3.9, 4.4]))  # n=5 < window
    assert list(out.columns) == ['date', 'sahm', 'triggered']
    assert len(out) == 5
    assert out['sahm'].isna().all()          # trough never fills
    assert not out['triggered'].any()
    assert out['triggered'].dtype == bool


@pytest.mark.parametrize('bad', [
    pd.DataFrame({'date': [], 'value': []}),                       # empty
    None,                                                          # None
    pd.DataFrame({'date': pd.date_range('2020-01-01', periods=3)}),  # missing 'value'
])
def test_degenerate_input_returns_canonical_empty_frame(bad):
    out = signals.sahm_rule(bad)
    assert list(out.columns) == ['date', 'sahm', 'triggered']
    assert len(out) == 0
    assert out['triggered'].dtype == bool
    assert not np.isinf(out['sahm'].to_numpy(dtype='float64')).any()


def test_all_nan_values_never_trigger_no_inf_no_raise():
    # Valid dates but all-NaN values: _to_dated_series (the pinned compare.py
    # body) drops NaT dates only, so the series is non-empty NaN -> every sahm
    # is NaN and every row is False. The contract is "never raise / never inf /
    # never trigger", not necessarily a 0-row frame.
    bad = pd.DataFrame({'date': pd.date_range('2020-01-01', periods=3),
                        'value': [np.nan, np.nan, np.nan]})
    out = signals.sahm_rule(bad)
    assert list(out.columns) == ['date', 'sahm', 'triggered']
    assert out['sahm'].isna().all()
    assert not out['triggered'].any()
    assert out['triggered'].dtype == bool
    assert not np.isinf(out['sahm'].to_numpy(dtype='float64')).any()


def test_no_inf_in_sahm_on_rising_and_flat():
    rising = signals.sahm_rule(
        _frame([3.5] * 15 + [3.6, 3.8, 4.1, 4.5, 5.0, 5.6, 6.3, 7.0, 8.0])
    )
    flat = signals.sahm_rule(_frame([4.0] * 24))
    assert not np.isinf(rising['sahm'].to_numpy(dtype='float64')).any()
    assert not np.isinf(flat['sahm'].to_numpy(dtype='float64')).any()


def test_embedded_nan_does_not_break_or_inf_and_still_fires():
    # Same rising shape but with a NaN dropped into an early flat month.
    vals = [3.5] * 15 + [3.6, 3.8, 4.1, 4.5, 5.0, 5.6, 6.3, 7.0, 8.0]
    vals[5] = np.nan
    out = signals.sahm_rule(_frame(vals))
    assert list(out.columns) == ['date', 'sahm', 'triggered']
    assert not np.isinf(out['sahm'].to_numpy(dtype='float64')).any()
    # min_periods counts non-NaN, so the trough/trigger is delayed, not broken;
    # the series still eventually fires.
    assert out['triggered'].any()


# --- threshold_cross_flag --------------------------------------------------

def test_threshold_cross_partitions_above_below_and_guards_non_finite():
    frame = _frame([1.0, np.nan, np.inf, -np.inf, 5.0, 2.0])
    above = signals.threshold_cross_flag(frame, level=3, direction='above')
    below = signals.threshold_cross_flag(frame, level=3, direction='below')

    assert list(above.columns) == ['date', 'value', 'flag']
    assert above['flag'].dtype == bool

    # above 3 -> only the 5.0 row.
    assert above['flag'].tolist() == [False, False, False, False, True, False]
    # below 3 -> only the 1.0 and 2.0 rows.
    assert below['flag'].tolist() == [True, False, False, False, False, True]

    # NaN / +inf / -inf NEVER flag in either direction (np.isfinite guard).
    nonfinite_idx = [1, 2, 3]
    assert not above['flag'].iloc[nonfinite_idx].any()
    assert not below['flag'].iloc[nonfinite_idx].any()


def test_threshold_cross_bad_direction_raises():
    frame = _frame([1.0, 2.0, 3.0])
    with pytest.raises(ValueError):
        signals.threshold_cross_flag(frame, level=2, direction='sideways')


@pytest.mark.parametrize('bad', [
    pd.DataFrame({'date': [], 'value': []}),
    None,
    pd.DataFrame({'date': pd.date_range('2020-01-01', periods=3)}),  # missing 'value'
])
def test_threshold_cross_degenerate_input_returns_canonical_empty_frame(bad):
    out = signals.threshold_cross_flag(bad, level=0, direction='above')
    assert list(out.columns) == ['date', 'value', 'flag']
    assert len(out) == 0
    assert out['flag'].dtype == bool
