# categories.py
"""Category grouping for the macro indicators, as a PURE stdlib module.

This is the leanest of the helper modules: NO pandas / numpy / sqlite / network /
scipy / plotly at module top (it does not even need pandas), the same import
discipline as compare.py / detect.py / alerts.py / export.py. It maps each
indicator NAME (the keys of etl.py's ``INDICATORS`` dict -- e.g. 'CPI',
'Unemployment Rate' -- NOT the FRED series IDs) to one of four macro categories,
and turns a flat list of indicator names into a FLAT options list suitable for a
Dash ``dcc.Dropdown``.

WHY a flat list with disabled header rows: Dash ``dcc.Dropdown`` does not support
HTML optgroups, so the grouped look is achieved by inserting a DISABLED header
row (a ``{'label': '-- Growth --', 'value': '__cat__Growth', 'disabled': True}``
entry) before each category's members. The ``__cat__`` value sentinel plus
``disabled: True`` means a header can never be selected. This is the only shape
that is both unit-testable AND directly droppable into the existing
``options=[...]`` slots in app/dashboard.py.

etl.py is deliberately NOT imported here: it does ``from fredapi import Fred`` and
constructs a network client at module top, so importing it would drag a network
dependency (and an absent FRED_API_KEY) into this pure module and into the tests.
The indicator names are therefore the responsibility of the caller (the Dash app
already has them in ``available_indicators``); this module only needs the
name->category map, which we duplicate below.
"""

# Each etl.py indicator NAME -> its macro category. (Names mirror etl.py's
# INDICATORS dict keys, not the FRED series IDs.) 'Consumer Sentiment' is treated
# as a growth/confidence proxy -> 'Growth'.
INDICATOR_CATEGORY = {
    'GDP': 'Growth',
    'Consumer Sentiment': 'Growth',
    'CPI': 'Inflation',
    'PCE Price Index': 'Inflation',
    'Unemployment Rate': 'Labor',
    'Fed Funds Rate': 'Rates',
}

# Deterministic display order for the category sections. Any category NOT in this
# list (e.g. the fallback bucket below, or a future addition) is emitted AFTER
# these four in sorted order.
CATEGORY_ORDER = ['Growth', 'Inflation', 'Labor', 'Rates']

# Fallback bucket name for an unknown / newly-added indicator, so it degrades to a
# visible 'Other' section instead of vanishing.
UNCATEGORIZED = 'Other'


def categorize(indicator):
    """Return the category for `indicator`, or ``UNCATEGORIZED`` if unknown.

    Keeps the name->category lookup in ONE place so an unknown / newly-added FRED
    series degrades to 'Other' instead of silently dropping out of the dropdown.
    """
    return INDICATOR_CATEGORY.get(indicator, UNCATEGORIZED)


def build_grouped_options(indicators, header_prefix='-- ', header_suffix=' --'):
    """Turn a flat iterable of indicator NAMES into a FLAT Dash options list.

    Returns a ``list[dict]`` ready to drop into a ``dcc.Dropdown(options=...)``
    where each category contributes one DISABLED HEADER row followed by its
    members. The shape per entry:

      * header:  ``{'label': f'{header_prefix}{cat}{header_suffix}',
                    'value': f'__cat__{cat}', 'disabled': True}``
      * member:  ``{'label': name, 'value': name}``

    Algorithm:
      1. Group the input names by :func:`categorize`.
      2. Emit sections in ``CATEGORY_ORDER`` first, then any extra categories
         (e.g. 'Other') in sorted order AFTER the canonical four, so the output
         order is fully deterministic.
      3. SKIP a category with no members (no empty headers).
      4. Members within a category are sorted alphabetically (deterministic; the
         DB feed is already sorted, but we do not rely on input order).
      5. Input names are deduplicated, so a repeated name never appears twice.

    An empty or ``None`` `indicators` returns ``[]`` (no headers, no crash).
    """
    if not indicators:
        return []

    # Group deduplicated names by category. A set drops duplicates; we sort the
    # members per category below, so first-seen order is irrelevant.
    grouped = {}
    for name in set(indicators):
        grouped.setdefault(categorize(name), []).append(name)

    if not grouped:
        return []

    # Canonical categories first (in CATEGORY_ORDER), then any extras (e.g.
    # 'Other') in sorted order, for a fully deterministic section order.
    extras = sorted(cat for cat in grouped if cat not in CATEGORY_ORDER)
    ordered_categories = [c for c in CATEGORY_ORDER if c in grouped] + extras

    options = []
    for category in ordered_categories:
        members = grouped[category]
        if not members:  # belt-and-suspenders: never emit an empty header
            continue
        options.append({
            'label': f'{header_prefix}{category}{header_suffix}',
            'value': f'__cat__{category}',
            'disabled': True,
        })
        for name in sorted(members):
            options.append({'label': name, 'value': name})
    return options


def attach_grouped_dropdowns(app, available_indicators):
    """Rewire app/dashboard.py's dropdowns to the category-grouped options.
    # YOUR TURN: app/dashboard.py currently feeds each indicator dropdown a flat
    # list via ``options=[{'label': ind, 'value': ind} for ind in
    # available_indicators]`` (the four sites at lines ~73, ~108, ~115 and the
    # compare pair). Replace each of those with
    # ``options=build_grouped_options(available_indicators)``. Keep every
    # dropdown's ``value=`` default a REAL indicator name, never a '__cat__'
    # header -- the existing ``available_indicators[0]`` default already satisfies
    # this since headers are not in that list. Do NOT edit app/dashboard.py from
    # here; this is the documented wiring boundary, mirroring
    # export.make_download_callback. No test yet.
    """
    raise NotImplementedError  # YOUR TURN
