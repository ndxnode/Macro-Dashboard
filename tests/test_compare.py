# test_compare.py
"""Unit tests for src/compare.py using synthetic, in-memory data (no FRED call).

Run with:  ./.venv/bin/python -m pytest tests/test_compare.py
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

# Make src/ importable the same way the Dash app does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import compare  # noqa: E402


def _make_series(start='2020-01-01', periods=36, values=None, freq='MS'):
    dates = pd.date_range(start=start, periods=periods, freq=freq)
    if values is None:
        values = np.arange(periods, dtype='float64')
    return pd.DataFrame({'date': dates, 'value': values})


def test_align_series_inner_join_keeps_shared_dates():
    a = _make_series(start='2020-01-01', periods=6)          # Jan..Jun
    b = _make_series(start='2020-03-01', periods=6)          # Mar..Aug
    aligned = compare.align_series(a, b, name_a='a', name_b='b')
    # Overlap is Mar, Apr, May, Jun -> 4 rows, no NaNs.
    assert list(aligned.columns) == ['a', 'b']
    assert len(aligned) == 4
    assert not aligned.isnull().any().any()


def test_align_series_outer_join_introduces_nans():
    a = _make_series(start='2020-01-01', periods=3)
    b = _make_series(start='2020-02-01', periods=3)
    aligned = compare.align_series(a, b, how='outer')
    # Union of Jan..Mar and Feb..Apr = 4 distinct months, with NaN gaps.
    assert len(aligned) == 4
    assert aligned.isnull().any().any()


def test_align_series_sorts_and_dedups():
    df = pd.DataFrame({
        'date': ['2020-03-01', '2020-01-01', '2020-02-01', '2020-02-01'],
        'value': [3.0, 1.0, 2.0, 99.0],  # duplicate Feb: last (99) should win
    })
    aligned = compare.align_series(df, df, name_a='x', name_b='y')
    assert list(aligned.index) == list(pd.to_datetime(['2020-01-01', '2020-02-01', '2020-03-01']))
    assert aligned['x'].tolist() == [1.0, 99.0, 3.0]


def test_pct_change_transform_values_and_drop():
    a = _make_series(values=[100.0, 110.0, 121.0], periods=3)
    aligned = compare.align_series(a, a, name_a='a', name_b='b')
    pct = compare.pct_change_transform(aligned)
    # First row dropped; +10% then +10%.
    assert len(pct) == 2
    np.testing.assert_allclose(pct['a'].values, [10.0, 10.0])


def test_rolling_correlation_perfectly_correlated_is_one():
    n = 24
    base = np.linspace(0, 10, n) + np.sin(np.linspace(0, 6, n))
    a = _make_series(values=base, periods=n)
    b = _make_series(values=2.0 * base + 5.0, periods=n)  # linear transform => corr 1
    aligned = compare.align_series(a, b, name_a='a', name_b='b')
    roll = compare.rolling_correlation(aligned, window=6)
    valid = roll.dropna()
    assert len(valid) > 0
    np.testing.assert_allclose(valid.values, 1.0, atol=1e-9)


def test_rolling_correlation_inverse_is_negative_one():
    n = 24
    base = np.linspace(0, 10, n) + np.cos(np.linspace(0, 6, n))
    a = _make_series(values=base, periods=n)
    b = _make_series(values=-base, periods=n)  # mirror => corr -1
    aligned = compare.align_series(a, b, name_a='a', name_b='b')
    roll = compare.rolling_correlation(aligned, window=6)
    valid = roll.dropna()
    assert len(valid) > 0
    np.testing.assert_allclose(valid.values, -1.0, atol=1e-9)


def test_build_comparison_bundle_keys_and_overall_corr():
    n = 30
    rng = np.random.default_rng(42)
    base = np.cumsum(rng.normal(size=n))
    a = _make_series(values=base, periods=n)
    b = _make_series(values=base + rng.normal(scale=0.01, size=n), periods=n)
    result = compare.build_comparison(a, b, name_a='a', name_b='b', window=12)

    assert set(result.keys()) == {
        'overlay', 'rolling_corr', 'overall_corr', 'n_overlap', 'window', 'pct_change'
    }
    assert isinstance(result['overlay'], pd.DataFrame)
    assert result['n_overlap'] == n
    assert result['pct_change'] is False
    # Near-identical series => strong positive overall correlation.
    assert result['overall_corr'] > 0.99


def test_build_comparison_pct_change_flag_changes_overlay_space():
    n = 12
    a = _make_series(values=np.linspace(100, 200, n), periods=n)
    b = _make_series(values=np.linspace(50, 150, n), periods=n)
    plain = compare.build_comparison(a, b, name_a='a', name_b='b', window=4)
    pct = compare.build_comparison(a, b, name_a='a', name_b='b', window=4, pct_change=True)
    assert plain['pct_change'] is False
    assert pct['pct_change'] is True
    # pct-change drops the first row.
    assert len(pct['overlay']) == len(plain['overlay']) - 1


def test_empty_inputs_are_handled_gracefully():
    empty = pd.DataFrame(columns=['date', 'value'])
    aligned = compare.align_series(empty, empty)
    assert aligned.empty
    result = compare.build_comparison(empty, empty)
    assert result['n_overlap'] == 0
    assert np.isnan(result['overall_corr'])


def test_pct_change_handles_zero_prior_value_without_inf():
    """A prior value of 0 makes raw pct_change emit +/-inf; the transform must
    turn those into NaN so they never reach a correlation. inf survives dropna
    and silently collapses every Pearson r to NaN, so this is a real hazard for
    macro data that legitimately touches zero (e.g. a rate at the zero bound)."""
    a = _make_series(values=[0.0, 10.0, 20.0, 40.0], periods=4)   # 0 -> 10 = +inf%
    b = _make_series(values=[1.0, 2.0, 3.0, 4.0], periods=4)
    aligned = compare.align_series(a, b, name_a='a', name_b='b')
    pct = compare.pct_change_transform(aligned)
    assert not np.isinf(pct.to_numpy()).any()
    # The first transformed row for column 'a' is inf->NaN; later rows are finite.
    assert np.isnan(pct['a'].iloc[0])
    assert np.isfinite(pct['a'].iloc[1:]).all()


def test_build_comparison_pct_change_with_zero_still_correlates():
    """With the inf scrubbed out there are still enough finite, paired points to
    compute a correlation -- before the fix the lone inf poisoned overall_corr
    and the whole rolling series to NaN despite valid overlapping data.

    The finite region carries real variance (the growth rates differ), so the
    test distinguishes "inf wiped out the result" from a legitimate zero-variance
    NaN. Without the fix overall_corr is NaN; with it the finite points correlate.
    """
    # 'a' starts at 0 (-> +inf% on the first step) then has varied growth rates.
    a = _make_series(values=[0.0, 10.0, 12.0, 18.0, 19.0, 26.0], periods=6)
    b = _make_series(values=[5.0, 10.0, 13.0, 17.0, 21.0, 24.0], periods=6)
    result = compare.build_comparison(
        a, b, name_a='a', name_b='b', window=3, pct_change=True
    )
    # inf row in 'a' is dropped pairwise; remaining finite points correlate.
    assert np.isfinite(result['overall_corr'])
    assert result['rolling_corr'].notna().any()


def test_rolling_correlation_zero_variance_window_is_nan():
    """The docstring promises constant (zero-variance) windows yield NaN rather
    than a spurious correlation value."""
    a = _make_series(values=[5.0] * 10, periods=10)              # constant
    b = _make_series(values=np.arange(10.0), periods=10)
    aligned = compare.align_series(a, b, name_a='a', name_b='b')
    roll = compare.rolling_correlation(aligned, window=4)
    assert roll.isna().all()
