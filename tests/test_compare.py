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
