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


# --- (g) summarize_alert: empty -> "No alerts." ------------------------------

def test_summarize_alert_empty_is_no_alerts():
    # A flat series fires no anomalies -> evaluate_alert returns 0 rows.
    df = _make_df([5.0] * 20)
    rule = alerts.AlertRule(indicator='X', kind='anomaly', threshold=3.0, window=6)
    fired = alerts.evaluate_alert(rule, df)
    assert len(fired) == 0
    assert alerts.summarize_alert(rule, fired) == 'No alerts.'
    # None is also treated as "no alerts" (keeps the fn total).
    assert alerts.summarize_alert(rule, None) == 'No alerts.'


# --- (h) summarize_alert: level 'above' summary, exact format locked ---------

def test_summarize_alert_level_above_exact_string():
    # [1,5,5,9,2,7] threshold 5 -> above fires {9,7}; sorted date-ASC, the LAST
    # row is the latest firing point (7.0 at 2020-06-01).
    df = _make_df([1.0, 5.0, 5.0, 9.0, 2.0, 7.0])
    rule = alerts.AlertRule(indicator='UNRATE', kind='level', threshold=5.0,
                            direction='above')
    fired = alerts.evaluate_alert(rule, df)
    assert len(fired) == 2  # two firing points -> plural "points"

    summary = alerts.summarize_alert(rule, fired)
    # Lock the EXACT format for at least one case.
    assert summary == 'UNRATE: 2 points above level 5 (latest 2020-06-01 = 7)'
    # And spell out the load-bearing parts that the exact string encodes.
    assert 'UNRATE' in summary                 # indicator name
    assert '2 points' in summary               # firing count + plural
    assert 'above level 5' in summary          # verb from kind/direction/threshold
    assert '2020-06-01' in summary             # latest firing date (the LAST row)
    assert '= 7' in summary                     # latest value
    # Level summaries do NOT render z (z_score is NaN for level rules).
    assert 'z=' not in summary


# --- (i) summarize_alert: anomaly summary includes the real z ----------------

def test_summarize_alert_anomaly_includes_real_z_and_singular():
    rng = np.random.default_rng(0)
    base = 10.0 + rng.normal(scale=0.1, size=40)
    spike_idx = 25
    base[spike_idx] = 30.0  # one positive spike -> exactly one firing point
    df = _make_df(base)
    rule = alerts.AlertRule(indicator='CPI', kind='anomaly', threshold=3.0,
                            window=12, direction='above')
    fired = alerts.evaluate_alert(rule, df)
    assert len(fired) == 1  # one firing point -> singular "point"

    summary = alerts.summarize_alert(rule, fired)
    assert 'CPI' in summary
    assert '1 point' in summary and '1 points' not in summary  # singular
    # The latest firing date is the spike's date (the only/last row).
    assert df['date'].iloc[spike_idx].strftime('%Y-%m-%d') in summary
    # Anomaly summaries DO render the real latest z (>3.0 here) -- locks that
    # anomaly summaries include z while level summaries (above) do not.
    assert 'z=' in summary
    assert fired['z_score'].iloc[-1] > 3.0


# --- (j) summarize_alert: 'both' direction verbs (level vs anomaly) ----------

def test_summarize_alert_both_direction_verbs():
    # level 'both' -> "off level <thr>"; one finite non-equal point fires.
    df = _make_df([5.0, 7.0])
    lvl_rule = alerts.AlertRule(indicator='Y', kind='level', threshold=5.0,
                               direction='both')
    lvl_fired = alerts.evaluate_alert(lvl_rule, df)
    assert 'off level 5' in alerts.summarize_alert(lvl_rule, lvl_fired)

    # anomaly 'both' -> "|z|>" verb; the abs-based outlier fires on the spike.
    rng = np.random.default_rng(2)
    base = 10.0 + rng.normal(scale=0.1, size=40)
    base[25] = 30.0
    adf = _make_df(base)
    an_rule = alerts.AlertRule(indicator='Z', kind='anomaly', threshold=3.0,
                              window=12, direction='both')
    an_fired = alerts.evaluate_alert(an_rule, adf)
    an_summary = alerts.summarize_alert(an_rule, an_fired)
    assert '|z|>3' in an_summary
    assert 'z=' in an_summary  # anomaly summaries carry the real z


# --- (j2) _describe_verb: anomaly/below negative threshold, no double-minus ---

def test_describe_verb_below_negative_threshold_no_double_minus():
    # threshold is taken by MAGNITUDE: the 'below' bound is the negative |z|, so a
    # negative threshold must NOT render "z=--3". Both -3 and 3 collapse to "z=-3".
    neg = alerts.AlertRule(indicator='X', kind='anomaly', direction='below',
                           threshold=-3)
    pos = alerts.AlertRule(indicator='X', kind='anomaly', direction='below',
                           threshold=3)
    assert alerts._describe_verb(neg) == 'anomalies below z=-3'   # was 'z=--3'
    assert alerts._describe_verb(pos) == 'anomalies below z=-3'   # byte-identical


# --- (k) build_alerts_payload: empty/None rules -> [] ------------------------

def test_build_alerts_payload_empty_or_none_rules_is_empty_list():
    frames = {'X': _make_df([1.0, 2.0, 3.0])}
    assert alerts.build_alerts_payload(frames, []) == []
    assert alerts.build_alerts_payload(frames, None) == []
    # Empty frames dict with no rules is also fine.
    assert alerts.build_alerts_payload({}, []) == []


# --- (l) build_alerts_payload: order preserved, matches standalone calls -----

def test_build_alerts_payload_order_and_matches_standalone():
    # Two rules over a frames dict; payload order == input order and each entry's
    # fired/summary byte-equals the standalone evaluate_alert/summarize_alert.
    df_a = _make_df([1.0, 5.0, 5.0, 9.0, 2.0, 7.0])
    df_b = _make_df([10.0, 20.0, 30.0, 40.0])
    frames = {'A': df_a, 'B': df_b}

    rule_a = alerts.AlertRule(indicator='A', kind='level', threshold=5.0,
                              direction='above')
    rule_b = alerts.AlertRule(indicator='B', kind='level', threshold=25.0,
                              direction='above')
    payload = alerts.build_alerts_payload(frames, [rule_a, rule_b])

    assert [p['indicator'] for p in payload] == ['A', 'B']  # input order preserved
    assert [p['rule'] for p in payload] == [rule_a, rule_b]

    for rule, df, entry in [(rule_a, df_a, payload[0]), (rule_b, df_b, payload[1])]:
        expected_fired = alerts.evaluate_alert(rule, df)
        expected_summary = alerts.summarize_alert(rule, expected_fired)
        pd.testing.assert_frame_equal(entry['fired'], expected_fired)
        assert entry['n_fired'] == len(entry['fired']) == len(expected_fired)
        assert entry['summary'] == expected_summary  # byte-equal


# --- (m) build_alerts_payload: missing indicator -> "No alerts.", no KeyError -

def test_build_alerts_payload_missing_indicator_degrades_no_keyerror():
    frames = {'A': _make_df([1.0, 9.0])}  # 'MISSING' is absent
    rule = alerts.AlertRule(indicator='MISSING', kind='level', threshold=5.0,
                            direction='above')
    payload = alerts.build_alerts_payload(frames, [rule])

    assert len(payload) == 1
    entry = payload[0]
    assert entry['indicator'] == 'MISSING'
    assert entry['n_fired'] == 0
    assert len(entry['fired']) == 0
    assert list(entry['fired'].columns) == ['date', 'value', 'z_score']
    assert entry['summary'] == 'No alerts.'
    # An explicit None frame value degrades the same way (no KeyError either).
    payload_none = alerts.build_alerts_payload({'MISSING': None}, [rule])
    assert payload_none[0]['summary'] == 'No alerts.'
    assert payload_none[0]['n_fired'] == 0


# --- (n) build_alerts_payload: a firing level rule locks n_fired + summary ----

def test_build_alerts_payload_firing_rule_locks_count_and_summary():
    # Reuse the [1,5,5,9,2,7] / threshold-5 fixture: level-above fires {9,7}.
    df = _make_df([1.0, 5.0, 5.0, 9.0, 2.0, 7.0])
    rule = alerts.AlertRule(indicator='UNRATE', kind='level', threshold=5.0,
                            direction='above')
    payload = alerts.build_alerts_payload({'UNRATE': df}, [rule])

    entry = payload[0]
    assert entry['n_fired'] == 2                       # two firing points, known
    assert entry['n_fired'] == len(entry['fired'])
    assert entry['summary'] != 'No alerts.'            # it fired
    assert 'above level 5' in entry['summary']         # non-"No alerts." substring
    assert entry['indicator'] == 'UNRATE'
