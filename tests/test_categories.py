# test_categories.py
"""Unit tests for src/categories.py's PURE category-grouping helpers.

These exercise categorize / build_grouped_options on a SYNTHETIC list of
indicator names. etl.py is deliberately NOT imported (it does
``from fredapi import Fred`` + builds a network client at module top); these tests
stay pure -- no scipy, no sqlite, no network, no pandas needed by the module.

Run with:  ./.venv/bin/python -m pytest tests/test_categories.py
"""
import os
import sys

# Make src/ importable the same way the Dash app (and the other tests) does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import categories  # noqa: E402


# A synthetic indicator list spanning >= 3 categories (Growth / Inflation /
# Rates here -- Labor deliberately omitted in some tests to check empty-section
# skipping). These mirror real etl.py names but the tests do not depend on etl.
SYNTH_THREE_CATS = ['GDP', 'CPI', 'Fed Funds Rate', 'Consumer Sentiment']


def _headers(options):
    """The category-header entries (disabled, __cat__-valued) in order."""
    return [o for o in options if o.get('disabled')]


def _selectable(options):
    """The non-header (selectable) entries in order."""
    return [o for o in options if not o.get('disabled')]


# ---- (1) categorize: known -> category, unknown -> UNCATEGORIZED ------------

def test_categorize_known_and_unknown():
    assert categories.categorize('GDP') == 'Growth'
    assert categories.categorize('Consumer Sentiment') == 'Growth'
    assert categories.categorize('CPI') == 'Inflation'
    assert categories.categorize('PCE Price Index') == 'Inflation'
    assert categories.categorize('Unemployment Rate') == 'Labor'
    assert categories.categorize('Fed Funds Rate') == 'Rates'
    # Unknown / newly-added series degrades to the 'Other' bucket, not vanishes.
    assert categories.categorize('Mystery Series') == categories.UNCATEGORIZED
    assert categories.UNCATEGORIZED == 'Other'


# ---- (2) build_grouped_options shape: flat dicts, disabled __cat__ headers --

def test_build_grouped_options_shape_and_values():
    options = categories.build_grouped_options(SYNTH_THREE_CATS)

    # Flat list of dicts.
    assert isinstance(options, list)
    assert all(isinstance(o, dict) for o in options)

    # Every header is disabled and carries a '__cat__'-prefixed value.
    headers = _headers(options)
    assert headers, 'expected at least one category header'
    for h in headers:
        assert h['disabled'] is True
        assert h['value'].startswith('__cat__')
        # Header label wraps the category name with the prefix/suffix.
        cat = h['value'][len('__cat__'):]
        assert h['label'] == f'-- {cat} --'

    # Selectable (non-header) values equal the input set (order aside).
    selectable_values = {o['value'] for o in _selectable(options)}
    assert selectable_values == set(SYNTH_THREE_CATS)
    # Selectable entries are plain {'label','value'} with label == value.
    for o in _selectable(options):
        assert o['label'] == o['value']
        assert 'disabled' not in o


# ---- (3) section order follows CATEGORY_ORDER, members alphabetical ---------

def test_section_order_and_member_sort():
    # Two Growth members (GDP, Consumer Sentiment) so we can check member sort.
    options = categories.build_grouped_options(
        ['Fed Funds Rate', 'CPI', 'GDP', 'Consumer Sentiment']
    )
    header_cats = [h['value'][len('__cat__'):] for h in _headers(options)]
    # Growth before Inflation before Rates (Labor absent here).
    assert header_cats == ['Growth', 'Inflation', 'Rates']

    # Within the Growth section, members are alphabetical:
    # 'Consumer Sentiment' < 'GDP'.
    labels = [o['label'] for o in options]
    growth_idx = labels.index('-- Growth --')
    inflation_idx = labels.index('-- Inflation --')
    growth_members = [
        o['value'] for o in options[growth_idx + 1:inflation_idx]
    ]
    assert growth_members == ['Consumer Sentiment', 'GDP']


# ---- (4) empty category produces NO header ----------------------------------

def test_empty_category_emits_no_header():
    # No 'Labor' / 'Rates' indicators in the input -> their headers must be absent.
    options = categories.build_grouped_options(['GDP', 'CPI'])
    header_cats = [h['value'][len('__cat__'):] for h in _headers(options)]
    assert 'Labor' not in header_cats
    assert 'Rates' not in header_cats
    assert header_cats == ['Growth', 'Inflation']


# ---- (5) empty list and None -> [] ------------------------------------------

def test_empty_and_none_inputs_return_empty_list():
    assert categories.build_grouped_options([]) == []
    assert categories.build_grouped_options(None) == []


# ---- (6) duplicate input name appears only once -----------------------------

def test_duplicate_input_name_deduplicated():
    options = categories.build_grouped_options(['GDP', 'GDP', 'CPI'])
    selectable_values = [o['value'] for o in _selectable(options)]
    assert selectable_values.count('GDP') == 1
    assert sorted(selectable_values) == ['CPI', 'GDP']


# ---- (7) unknown indicator -> 'Other' section sorted AFTER the canonical four -

def test_unknown_indicator_lands_in_other_section_last():
    options = categories.build_grouped_options(
        ['GDP', 'Unemployment Rate', 'Mystery Series', 'CPI', 'Fed Funds Rate']
    )
    header_cats = [h['value'][len('__cat__'):] for h in _headers(options)]
    # All four canonical categories present (one member each) and 'Other' LAST.
    assert header_cats == ['Growth', 'Inflation', 'Labor', 'Rates', 'Other']

    # The unknown name is the sole member of the 'Other' section.
    labels = [o['label'] for o in options]
    other_idx = labels.index('-- Other --')
    other_members = [o['value'] for o in options[other_idx + 1:]]
    assert other_members == ['Mystery Series']


# ---- (8) first_real_value: first selectable value, never a __cat__ header ----

def test_first_real_value_returns_first_selectable_indicator():
    options = categories.build_grouped_options(SYNTH_THREE_CATS)
    result = categories.first_real_value(options)
    # The default must be a REAL indicator from the input -- never a header.
    assert result in set(SYNTH_THREE_CATS)
    assert not str(result).startswith('__cat__')
    # It is exactly the first SELECTABLE entry (options[0] is a disabled header).
    first_selectable = next(o['value'] for o in options if not o.get('disabled'))
    assert result == first_selectable


def test_first_real_value_empty_and_none_return_default():
    assert categories.first_real_value([]) is None
    assert categories.first_real_value(None) is None
    # A custom default is honored on empty / None input.
    assert categories.first_real_value([], default='X') == 'X'
    assert categories.first_real_value(None, default='X') == 'X'


def test_first_real_value_is_never_a_cat_header():
    # The whole point: on real grouped options the default is never a header.
    options = categories.build_grouped_options(
        ['GDP', 'CPI', 'Fed Funds Rate', 'Unemployment Rate']
    )
    result = categories.first_real_value(options)
    assert result is not None
    assert not str(result).startswith('__cat__')


def test_first_real_value_all_headers_returns_default():
    # A synthetic options list that is ALL disabled headers -> default.
    all_headers = [
        {'label': '-- X --', 'value': '__cat__X', 'disabled': True},
        {'label': '-- Y --', 'value': '__cat__Y', 'disabled': True},
    ]
    assert categories.first_real_value(all_headers) is None
    assert categories.first_real_value(all_headers, default='fallback') == 'fallback'


# ---- (extra) the YOUR TURN UI hook is importable but unimplemented -----------

def test_attach_grouped_dropdowns_is_a_your_turn_stub():
    import pytest
    with pytest.raises(NotImplementedError):
        categories.attach_grouped_dropdowns(None, ['GDP'])
