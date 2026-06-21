# test_freshness.py
"""Unit tests for src/freshness.py's PURE data-freshness / staleness summary.

These exercise ``summarize_freshness`` directly on synthetic frames -- no
sqlite, no Dash, no network, no kaleido. Importing ``freshness`` here also
proves the module stays import-pure (only pandas/numpy enter; see the
fresh-subprocess probe at the bottom).

Run with:  ./.venv/bin/python -m pytest tests/test_freshness.py
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd

# Make src/ importable the same way the Dash app (and the sibling tests) do.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import freshness  # noqa: E402


def _frame(values, dates):
    return pd.DataFrame({'date': pd.to_datetime(list(dates)), 'value': values})


def _by_name(rows):
    return {r['indicator']: r for r in rows}


def test_global_max_default_flags_lagging_indicator():
    # A latest 2020-03-01, B latest 2020-01-15 (46d behind). With NO as_of, the
    # GLOBAL max (2020-03-01) is the reference -> A is fresh (age 0), B is stale
    # (age 46 > 45) -- the "one indicator's cron ETL went stale" signal.
    frames = {
        'A': _frame([1.0, 2.0, 3.0], ['2020-01-01', '2020-02-01', '2020-03-01']),
        'B': _frame([1.0, 2.0], ['2019-12-15', '2020-01-15']),
    }
    rows = freshness.summarize_freshness(frames)
    assert [r['indicator'] for r in rows] == ['A', 'B']
    by = _by_name(rows)

    assert by['A']['last_date'] == pd.Timestamp('2020-03-01')
    assert by['A']['age_days'] == 0
    assert by['A']['is_stale'] is False
    assert by['A']['n_points'] == 3

    assert by['B']['last_date'] == pd.Timestamp('2020-01-15')
    assert by['B']['age_days'] == 46
    assert by['B']['is_stale'] is True
    assert by['B']['n_points'] == 2


def test_explicit_as_of_shadows_global_max_default():
    # An explicit as_of (a str) overrides the global-max default: A last
    # 2020-03-01 vs as_of '2020-04-30' -> age 60, stale. Confirms as_of wins.
    frames = {
        'A': _frame([1.0, 2.0, 3.0], ['2020-01-01', '2020-02-01', '2020-03-01']),
    }
    rows = freshness.summarize_freshness(frames, as_of='2020-04-30')
    by = _by_name(rows)
    assert by['A']['age_days'] == 60
    assert by['A']['is_stale'] is True


def test_staleness_boundary_is_strictly_greater_than():
    # is_stale = age > stale_after_days. With stale_after_days=45:
    # exactly 45 days old is NOT stale (45 > 45 is False); 46 days IS stale.
    # ref (global max) = 2020-03-01; the 45d-before date is 2020-01-16, 46d is 2020-01-15.
    frames = {
        'fresh': _frame([9.0], ['2020-03-01']),       # drives the global max
        'edge45': _frame([1.0], ['2020-01-16']),      # exactly 45d behind
        'edge46': _frame([1.0], ['2020-01-15']),      # 46d behind
    }
    by = _by_name(freshness.summarize_freshness(frames))
    assert by['edge45']['age_days'] == 45
    assert by['edge45']['is_stale'] is False
    assert by['edge46']['age_days'] == 46
    assert by['edge46']['is_stale'] is True


def test_empty_missing_cols_and_all_nat_frames_emit_sentinel_rows():
    # Empty, missing-cols, and all-NaT-date frames each still EMIT one row with
    # the documented sentinel (last_date None, age_days None, n_points 0,
    # is_stale False) and never raise. A real frame G provides the global max.
    frames = {
        'G': _frame([1.0, 2.0], ['2020-02-01', '2020-03-01']),
        'empty': pd.DataFrame({'date': [], 'value': []}),
        'missing': pd.DataFrame({'foo': [1, 2], 'bar': [3, 4]}),     # no date/value cols
        'allnat': pd.DataFrame({'date': ['nope', 'bad'], 'value': [1.0, 2.0]}),
    }
    # The 'allnat' frame deliberately feeds un-parseable strings to the module's
    # ``pd.to_datetime(errors='coerce')`` (the all-NaT path under test); pandas
    # emits an inference UserWarning that is expected here, so silence it.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        rows = freshness.summarize_freshness(frames)
    assert [r['indicator'] for r in rows] == ['G', 'allnat', 'empty', 'missing']
    by = _by_name(rows)
    for key in ('empty', 'missing', 'allnat'):
        assert by[key]['last_date'] is None
        assert by[key]['age_days'] is None
        assert by[key]['n_points'] == 0
        assert by[key]['is_stale'] is False
    # The real frame is unaffected.
    assert by['G']['last_date'] == pd.Timestamp('2020-03-01')
    assert by['G']['n_points'] == 2


def test_n_points_counts_valid_date_rows_even_with_nan_values():
    # A frame with 2 valid-DATE rows whose VALUES are all NaN -> n_points 2,
    # last_date the max valid date. A NaN value does NOT make the row absent for
    # FRESHNESS (freshness is about DATES). With an explicit as_of so the global
    # max does not collapse age to 0.
    frames = {
        'nanvals': _frame([np.nan, np.nan], ['2020-01-01', '2020-02-01']),
    }
    rows = freshness.summarize_freshness(frames, as_of='2020-02-01')
    by = _by_name(rows)
    assert by['nanvals']['n_points'] == 2
    assert by['nanvals']['last_date'] == pd.Timestamp('2020-02-01')
    assert by['nanvals']['age_days'] == 0
    assert by['nanvals']['is_stale'] is False


def test_none_and_empty_mapping_return_empty_list_no_raise():
    # A falsy mapping (None AND {}) collapses to [] -- guarded BEFORE .keys()
    # (None has no .keys()); mirrors corr_matrix / alerts tolerance.
    assert freshness.summarize_freshness(None) == []
    assert freshness.summarize_freshness({}) == []


def test_sorted_name_order_and_single_indicator():
    # Keys inserted OUT of order -> output is sorted by indicator name.
    frames = {
        'zulu': _frame([1.0], ['2020-03-01']),
        'alpha': _frame([1.0], ['2020-03-01']),
        'mike': _frame([1.0], ['2020-03-01']),
    }
    rows = freshness.summarize_freshness(frames)
    assert [r['indicator'] for r in rows] == ['alpha', 'mike', 'zulu']

    # A single-indicator frame: the global max IS its own max -> age 0, not stale.
    solo = freshness.summarize_freshness({'only': _frame([1.0, 2.0],
                                          ['2020-01-01', '2020-02-01'])})
    assert len(solo) == 1
    assert solo[0]['indicator'] == 'only'
    assert solo[0]['age_days'] == 0
    assert solo[0]['is_stale'] is False


def test_all_empty_frames_global_max_none_no_crash():
    # If EVERY frame is empty/all-NaT, the global max is None -> every age_days
    # is None, every is_stale False, no crash (the None-age guard).
    frames = {
        'e1': pd.DataFrame({'date': [], 'value': []}),
        'e2': pd.DataFrame({'date': ['bad'], 'value': [1.0]}),  # all-NaT
    }
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)  # expected all-NaT coerce warning
        rows = freshness.summarize_freshness(frames)
    assert [r['indicator'] for r in rows] == ['e1', 'e2']
    for r in rows:
        assert r['last_date'] is None
        assert r['age_days'] is None
        assert r['is_stale'] is False
        assert r['n_points'] == 0


def test_import_purity():
    # freshness adds ONLY pandas/numpy -- no sqlite3/dash/plotly/kaleido/scipy.
    # Probe in a FRESH subprocess: the pytest session itself drags dash/plotly
    # into this process's sys.modules (other test modules / plugins), so a clean
    # interpreter is the only honest measure of what `import freshness` pulls.
    import subprocess

    src_dir = os.path.join(os.path.dirname(__file__), '..', 'src')
    probe = (
        'import sys; sys.path.insert(0, %r); import freshness; '
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
