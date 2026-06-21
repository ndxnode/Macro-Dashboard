# test_detect.py
"""Unit tests for src/detect.py's PURE rolling-z-score core (no scipy, no sqlite,
no network).

These exercise the local-context anomaly detection directly on synthetic series.
Importing ``detect`` here also proves the scipy import is gone -- scipy's native
extension fails to dlopen in this environment, so any lingering top-level
``from scipy...`` would make this whole module fail to collect.

Run with:  ./.venv/bin/python -m pytest tests/test_detect.py
"""
import os
import sys

import numpy as np
import pandas as pd

# Make src/ importable the same way the Dash app (and test_compare) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import detect  # noqa: E402


def _dated(values, start='2020-01-01', freq='MS'):
    """Wrap a list/array of values in a date-indexed float Series."""
    values = np.asarray(values, dtype='float64')
    idx = pd.date_range(start=start, periods=len(values), freq=freq)
    return pd.Series(values, index=idx, name='value')


# --- (a) one obvious LOCAL spike is flagged; calm points are not -------------

def test_local_spike_is_flagged_and_calm_points_are_not():
    # Calm, low-noise baseline around 10, with one big spike partway through.
    rng = np.random.default_rng(0)
    base = 10.0 + rng.normal(scale=0.1, size=40)
    spike_idx = 25
    base[spike_idx] = 30.0  # huge local deviation
    s = _dated(base)

    out = detect.detect_anomalies(s, window=12, z_threshold=3.0)

    # The spike must be flagged.
    assert bool(out['is_outlier'].iloc[spike_idx]) is True
    # A calm point with enough history must not be flagged.
    assert bool(out['is_outlier'].iloc[20]) is False
    # Exactly one anomaly in this construction.
    assert int(out['is_outlier'].sum()) == 1


# --- (b) steady trend: rolling z stays in-band where a GLOBAL z over-flags ----

def test_trend_rolling_zscore_beats_global_zscore():
    # A long steady linear trend. A GLOBAL z-score (one mean/std for the whole
    # series) assigns large magnitudes to the endpoints; the rolling z-score
    # only ever sees a locally-near-linear window, so it stays small.
    n = 120
    s = _dated(np.arange(n, dtype='float64') * 2.0)  # perfectly linear

    z_roll = detect.rolling_zscore(s, window=12)
    valid = z_roll.dropna()
    assert len(valid) > 0
    # Local window of a straight line -> modest, bounded z everywhere.
    assert valid.abs().max() < 3.0

    # Contrast with a naive global z-score: its endpoints blow past the rolling
    # bound, demonstrating the "local context" win.
    arr = s.to_numpy()
    global_z = (arr - arr.mean()) / arr.std(ddof=0)
    assert np.abs(global_z).max() > valid.abs().max()

    # And detect_anomalies on a clean trend flags nothing.
    out = detect.detect_anomalies(s, window=12, z_threshold=3.0)
    assert int(out['is_outlier'].sum()) == 0


# --- (c) zero-variance / constant window: z is NaN (not inf), zero anomalies --

def test_constant_series_zero_variance_is_nan_not_inf():
    s = _dated([5.0] * 20)  # flat -> every trailing window has std 0

    z = detect.rolling_zscore(s, window=6)
    # 0/0 must be NaN, never +/-inf.
    assert not np.isinf(z.to_numpy()).any()
    assert z.isna().all()

    out = detect.detect_anomalies(s, window=6, z_threshold=3.0)
    # NaN must not compare > threshold, so no spurious anomaly rows.
    assert int(out['is_outlier'].sum()) == 0
    assert not np.isinf(out['z_score'].to_numpy()).any()


def test_flat_window_then_jump_does_not_emit_inf():
    # A flat run (std 0) followed by a jump is the classic (deviation)/0 -> inf
    # hazard. The jump's own window is no longer flat, so it can score, but no
    # value anywhere may be +/-inf.
    s = _dated([5.0] * 8 + [99.0] + [5.0] * 8)
    z = detect.rolling_zscore(s, window=4)
    assert not np.isinf(z.to_numpy()).any()
    out = detect.detect_anomalies(s, window=4, z_threshold=3.0)
    assert not np.isinf(out['z_score'].to_numpy()).any()


# --- (d) short series (len < window): clamps, no crash, no inf, early NaN ------

def test_short_series_clamps_without_crash():
    s = _dated([1.0, 5.0, 2.0])  # only 3 points, ask for window=12
    z = detect.rolling_zscore(s, window=12)

    assert len(z) == 3
    assert not np.isinf(z.to_numpy()).any()
    # eff_window clamps to 2 (max(2, min(12, 3))) with min_periods == eff_window,
    # so the very first row has < 2 observations and is NaN.
    assert np.isnan(z.iloc[0])
    # Later rows now have enough local history to be scored.
    assert z.iloc[1:].notna().any()

    # detect_anomalies on short data does not crash and returns aligned rows.
    out = detect.detect_anomalies(s, window=12, z_threshold=3.0)
    assert len(out) == 3
    assert list(out.columns) == ['value', 'z_score', 'is_outlier']


# --- (e) empty series: returns empty, no exception ---------------------------

def test_empty_series_returns_empty():
    s = pd.Series([], dtype='float64')
    z = detect.rolling_zscore(s, window=12)
    assert isinstance(z, pd.Series)
    assert z.empty

    out = detect.detect_anomalies(s, window=12, z_threshold=3.0)
    assert isinstance(out, pd.DataFrame)
    assert out.empty


# --- extra: non-numeric coercion and array-like input ------------------------

def test_non_numeric_values_coerced_to_nan():
    z = detect.rolling_zscore(['1', '2', 'oops', '4', '5'], window=3)
    assert len(z) == 5
    # The coerced-NaN position is NaN; no inf anywhere.
    assert np.isnan(z.iloc[2])
    assert not np.isinf(z.to_numpy()).any()


def test_array_like_input_is_accepted():
    # Plain numpy array (no index) should work and stay aligned. A noisy (not
    # perfectly flat) baseline gives the spike's trailing window a small but
    # nonzero std so the spike clears the threshold -- a lone point in a flat
    # window is capped at sqrt(window-1) sigma by construction.
    rng = np.random.default_rng(7)
    arr = 10.0 + rng.normal(scale=0.1, size=31)
    arr[15] = 40.0
    out = detect.detect_anomalies(arr, window=12, z_threshold=3.0)
    assert len(out) == len(arr)
    assert bool(out['is_outlier'].iloc[15]) is True


# --- name-smoke: the __main__ helper name is the SHORT one, not the typo -----

def test_calc_and_store_helper_name_is_canonical():
    # detect.py's __main__ block used to call the LONG name
    # `calculate_and_store_anomalies_for_indicator`, which does not exist (the
    # real fn at line ~99 is the SHORT `calc_and_store_anomalies_for_indicator`)
    # -> a NameError on a script run. Lock the canonical short name in and assert
    # the typo is gone so it can't silently creep back.
    assert hasattr(detect, 'calc_and_store_anomalies_for_indicator')
    assert callable(detect.calc_and_store_anomalies_for_indicator)
    assert not hasattr(detect, 'calculate_and_store_anomalies_for_indicator')
