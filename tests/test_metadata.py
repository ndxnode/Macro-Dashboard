# test_metadata.py
"""Unit tests for src/metadata.py's PURE per-indicator metadata helpers.

These exercise INDICATOR_META / DEFAULT_META and the indicator_meta / units_of /
describe / meta_for accessors. etl.py is deliberately NOT imported (it does
``from fredapi import Fred`` + builds a network client at module top); these tests
stay pure -- no scipy, no sqlite, no network, no pandas needed by the module.

Run with:  ./.venv/bin/python -m pytest tests/test_metadata.py
"""
import os
import sys

# Make src/ importable the same way the Dash app (and the other tests) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import metadata  # noqa: E402


# The canonical six etl.py indicator NAMES, as a literal tuple mirroring
# etl.INDICATORS keys (not the FRED series IDs). The tests assert metadata.py
# covers EXACTLY these and degrades gracefully for anything else.
ETL_INDICATOR_NAMES = (
    'CPI',
    'Unemployment Rate',
    'Fed Funds Rate',
    'GDP',
    'PCE Price Index',
    'Consumer Sentiment',
)

ENTRY_KEYS = {'series_id', 'units', 'frequency', 'description', 'source'}


# ---- (1) known accessors -- units / describe -------------------------------

def test_known_units_and_describe():
    assert metadata.units_of('CPI') == 'Index 1982-84=100'
    assert metadata.units_of('Unemployment Rate') == 'Percent'
    assert metadata.units_of('Fed Funds Rate') == 'Percent'
    # describe returns the entry's description verbatim.
    assert metadata.describe('GDP') == metadata.INDICATOR_META['GDP']['description']
    assert 'Gross Domestic Product' in metadata.describe('GDP')
    # indicator_meta returns the actual stored entry for a known name.
    assert metadata.indicator_meta('CPI') is metadata.INDICATOR_META['CPI']


# ---- (2) every etl.py indicator has metadata (exact key-set per entry) ------

def test_every_etl_indicator_has_metadata():
    for name in ETL_INDICATOR_NAMES:
        assert name in metadata.INDICATOR_META, name
        entry = metadata.INDICATOR_META[name]
        assert isinstance(entry, dict)
        assert set(entry.keys()) == ENTRY_KEYS, name
    # No EXTRA / stray names beyond the canonical six.
    assert set(metadata.INDICATOR_META.keys()) == set(ETL_INDICATOR_NAMES)


# ---- (3) unknown name -> default, no KeyError -------------------------------

def test_unknown_name_falls_back_to_default():
    meta = metadata.indicator_meta('Mystery Series')
    # Same shape as a real entry -- callers can index any key without a guard.
    assert set(meta.keys()) == ENTRY_KEYS
    assert meta['units'] == metadata.DEFAULT_META['units']
    # units_of degrades to the non-empty default unit; describe to the placeholder.
    assert metadata.units_of('Mystery Series') == metadata.DEFAULT_META['units']
    assert metadata.units_of('Mystery Series')  # non-empty
    assert metadata.describe('Mystery Series') == metadata.DEFAULT_META['description']
    # An explicit default= is returned for an unknown name (totality, no KeyError).
    sentinel = {'series_id': 'X', 'units': 'u', 'frequency': 'f',
                'description': 'd', 'source': 'FRED'}
    assert metadata.indicator_meta('Mystery Series', default=sentinel) is sentinel
    # ... but a KNOWN name ignores the default and returns its real entry.
    assert metadata.indicator_meta('CPI', default=sentinel) is metadata.INDICATOR_META['CPI']


# ---- (4) dict shape / non-empty contract for every entry --------------------

def test_entry_shape_and_non_empty_contract():
    for name, entry in metadata.INDICATOR_META.items():
        assert entry['source'] == 'FRED', name
        assert isinstance(entry['units'], str) and entry['units'].strip(), name
        assert isinstance(entry['series_id'], str) and entry['series_id'].strip(), name
        assert isinstance(entry['description'], str) and entry['description'].strip(), name
        assert isinstance(entry['frequency'], str) and entry['frequency'].strip(), name
    # DEFAULT_META has the same key-set, a non-empty units default, source FRED.
    assert set(metadata.DEFAULT_META.keys()) == ENTRY_KEYS
    assert metadata.DEFAULT_META['units'].strip()
    assert metadata.DEFAULT_META['source'] == 'FRED'


# ---- (5) accessor totality + fresh-copy + meta_for alias --------------------

def test_unknown_returns_fresh_copy_and_meta_for_alias():
    # An unknown lookup is a FRESH copy, NOT the module constant -- mutating it
    # must not corrupt DEFAULT_META or a later call.
    one = metadata.indicator_meta('Nope')
    assert one is not metadata.DEFAULT_META
    one['units'] = 'MUTATED'
    assert metadata.DEFAULT_META['units'] != 'MUTATED'
    two = metadata.indicator_meta('Nope')
    assert two['units'] == metadata.DEFAULT_META['units']  # unaffected by the mutation
    assert two is not one
    # meta_for is the indicator_meta alias -- identical object, identical behaviour.
    assert metadata.meta_for is metadata.indicator_meta
    assert metadata.meta_for('CPI') is metadata.indicator_meta('CPI')
    assert metadata.meta_for('Nope').keys() == metadata.indicator_meta('Nope').keys()


# ---- (6) import-purity (in-process stdlib probe, like test_categories) ------

def test_import_purity_stdlib_only():
    # metadata is stdlib-only, so an in-process probe suffices (no subprocess
    # needed): assert none of the heavy libs entered sys.modules. (Other test
    # modules may have dragged some in, so we re-import metadata in isolation by
    # asserting the module itself references none of them -- the strongest cheap
    # check is that a fresh attribute access does not import them. Here we assert
    # metadata exposes no heavy module object and that its own globals are clean.)
    heavy = {'pandas', 'numpy', 'plotly', 'dash', 'sqlite3', 'kaleido', 'scipy'}
    # metadata's own module namespace must not bind any heavy module.
    import types
    for value in vars(metadata).values():
        if isinstance(value, types.ModuleType):
            assert value.__name__.split('.')[0] not in heavy, value.__name__
