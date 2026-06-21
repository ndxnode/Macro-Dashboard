# test_corr_matrix.py
"""Unit tests for src/corr_matrix.py's PURE pairwise-correlation matrix.

These exercise the correlation builder directly on synthetic series -- no
sqlite, no Dash, no network, no kaleido. Importing ``corr_matrix`` here also
proves the module stays import-pure (only pandas/numpy enter).

Run with:  ./.venv/bin/python -m pytest tests/test_corr_matrix.py
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

# Make src/ importable the same way the Dash app (and test_signals) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import corr_matrix  # noqa: E402


def _frame(values, start='2020-01-01', freq='MS'):
    dates = pd.date_range(start=start, periods=len(values), freq=freq)
    return pd.DataFrame({'date': dates, 'value': values})


def test_correlated_anti_independent_triple():
    # On a shared 10-month window: B = 2A+1 (perfect +1), C = -A+5 (perfect -1),
    # D = a fixed-seed random series (independent of A -> |corr| small).
    a = np.arange(10, dtype='float64')
    frames = {
        'A': _frame(a),
        'B': _frame(2 * a + 1),
        'C': _frame(-a + 5),
        'D': _frame(np.random.default_rng(0).normal(size=10)),
    }
    M = corr_matrix.correlation_matrix(frames)

    # Sorted-square shape: index == columns == sorted names.
    assert list(M.index) == ['A', 'B', 'C', 'D']
    assert list(M.columns) == ['A', 'B', 'C', 'D']

    # Sign + magnitude of the deterministic pairs.
    assert M.loc['A', 'B'] == pytest.approx(1.0)
    assert M.loc['A', 'C'] == pytest.approx(-1.0)
    assert M.loc['B', 'C'] == pytest.approx(-1.0)
    # Independent (fixed-seed) pair is weakly correlated.
    assert abs(M.loc['A', 'D']) < 0.7

    # Symmetric and diagonal exactly 1.0.
    assert np.allclose(M.values, M.values.T, equal_nan=True)
    assert np.array_equal(np.diag(M.values), np.ones(4))


def test_min_overlap_drives_nan_guard():
    # A spans 2020-01..10; D shares only 2 dates (2020-09, 2020-10) with it.
    a = np.arange(10, dtype='float64')
    frames = {
        'A': _frame(a),
        'D': _frame([1.0, 2.0], start='2020-09-01'),
    }

    # min_overlap=3 -> the 2-date pair is undefined (NaN), diagonal still 1.0.
    M3 = corr_matrix.correlation_matrix(frames, min_overlap=3)
    assert np.isnan(M3.loc['A', 'D'])
    assert np.isnan(M3.loc['D', 'A'])
    assert np.array_equal(np.diag(M3.values), np.ones(2))

    # min_overlap=2 -> the same 2-date pair is now DEFINED (min_periods drives it).
    M2 = corr_matrix.correlation_matrix(frames, min_overlap=2)
    assert not np.isnan(M2.loc['A', 'D'])


def test_empty_dict_returns_empty_frame_no_raise():
    M = corr_matrix.correlation_matrix({})
    assert isinstance(M, pd.DataFrame)
    assert M.shape == (0, 0)


def test_single_indicator_is_1x1_unit():
    M = corr_matrix.correlation_matrix({'A': _frame(np.arange(10, dtype='float64'))})
    assert list(M.index) == ['A']
    assert list(M.columns) == ['A']
    assert M.shape == (1, 1)
    assert M.loc['A', 'A'] == 1.0


def test_degenerate_series_keep_diagonal_one():
    # Z is all-NaN values, K is a zero-variance constant -- both have a NaN
    # self-corr from .corr alone; Decision 3 forces their diagonals to 1.0.
    a = np.arange(10, dtype='float64')
    frames = {
        'A': _frame(a),
        'K': _frame([4.0] * 10),                 # zero variance (std == 0)
        'Z': _frame([np.nan] * 10),              # all-NaN values
    }
    M = corr_matrix.correlation_matrix(frames)

    assert list(M.index) == ['A', 'K', 'Z']
    assert list(M.columns) == ['A', 'K', 'Z']

    # Diagonal forced to 1.0 even for the degenerate series.
    assert M.loc['K', 'K'] == 1.0
    assert M.loc['Z', 'Z'] == 1.0
    assert M.loc['A', 'A'] == 1.0

    # Off-diagonals to a zero-variance / all-NaN series are NaN (no raise/inf).
    assert np.isnan(M.loc['A', 'K'])
    assert np.isnan(M.loc['A', 'Z'])
    assert not np.isinf(M.to_numpy(dtype='float64')).any()

    # Still a symmetric, full sorted square.
    assert np.allclose(M.values, M.values.T, equal_nan=True)


def test_non_overlapping_series_off_diagonal_nan():
    # Disjoint date windows -> zero shared dates -> off-diagonal NaN, no raise.
    frames = {
        'A': _frame(np.arange(6, dtype='float64'), start='2020-01-01'),
        'B': _frame(np.arange(6, dtype='float64'), start='2021-01-01'),
    }
    M = corr_matrix.correlation_matrix(frames)
    assert np.isnan(M.loc['A', 'B'])
    assert np.isnan(M.loc['B', 'A'])
    assert np.array_equal(np.diag(M.values), np.ones(2))


def test_import_purity():
    # corr_matrix adds ONLY pandas/numpy -- no sqlite3/dash/plotly/kaleido/scipy.
    # Probe in a FRESH subprocess: the pytest session itself drags dash/plotly
    # into this process's sys.modules (other test modules / plugins), so a clean
    # interpreter is the only honest measure of what `import corr_matrix` pulls.
    import subprocess

    src_dir = os.path.join(os.path.dirname(__file__), '..', 'src')
    probe = (
        'import sys; sys.path.insert(0, %r); import corr_matrix; '
        "bad=[m for m in sys.modules if m.split('.')[0] in "
        "('sqlite3','dash','plotly','kaleido','scipy')]; "
        'assert not bad, bad; print("pure")'
    ) % src_dir
    result = subprocess.run(
        [sys.executable, '-c', probe],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'pure'
