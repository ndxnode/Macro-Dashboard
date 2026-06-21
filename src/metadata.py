# metadata.py
"""Per-indicator display metadata for the macro indicators, as a PURE stdlib module.

This is the natural companion to src/categories.py: where ``categories`` maps each
indicator NAME -> a macro category, ``metadata`` maps each indicator NAME -> a small
dict of display metadata ``{series_id, units, frequency, description, source}``. Both
are zero-dependency lookup tables that feed the Dash UI -- categories drives the
grouped dropdown; metadata drives axis labels, hover tooltips and CSV-export headers.

NO pandas / numpy / plotly / dash / sqlite3 / kaleido / scipy at module top (it does
not even need pandas), exactly the import discipline of categories.py / compare.py /
detect.py. A plain ``import metadata`` therefore pulls in nothing heavy.

The keys are the etl.py indicator NAMES -- the keys of ``etl.INDICATORS`` (e.g.
'CPI', 'Unemployment Rate') -- NOT the FRED series IDs (the series IDs live in each
entry's ``'series_id'``). etl.py is deliberately NOT imported here: it does
``from fredapi import Fred`` and constructs a network client at module top, so
importing it would drag a network dependency (and an absent FRED_API_KEY) into this
pure module and into the tests. The six names are therefore duplicated here ON
PURPOSE; they are cross-checked against etl.INDICATORS / categories.INDICATOR_CATEGORY
(same six) by the tests.

Accessors mirror ``categories.categorize``'s ``.get(key, default)`` totality idiom:
they degrade to a default and NEVER raise ``KeyError`` on an unknown / newly-added
indicator name, so a future FRED series that has not been described yet still renders
a safe non-empty axis label and a placeholder description instead of crashing the UI.
"""

# The canonical unknown / fallback entry. It has the SAME keys as a real entry so a
# caller can index ANY key (e.g. ``indicator_meta(x)['units']``) without a guard. A
# non-empty 'units' ('Value') is a safe default axis label; the tests assert units is
# always non-empty.
DEFAULT_META = {
    'series_id': None,
    'units': 'Value',
    'frequency': 'Unknown',
    'description': 'No description available.',
    'source': 'FRED',
}

# Each etl.py indicator NAME -> its FRED display metadata. The NAMES + series_ids are
# verbatim from etl.INDICATORS (CPI/CPIAUCSL, Unemployment Rate/UNRATE, Fed Funds
# Rate/FEDFUNDS, GDP/GDP, PCE Price Index/PCEPI, Consumer Sentiment/UMCSENT) -- the
# same six keys as categories.INDICATOR_CATEGORY. Units / frequency are standard
# FRED-consistent labels for these series; every 'source' is 'FRED'. No network is
# touched to build this -- it is static, factual metadata.
INDICATOR_META = {
    'CPI': {
        'series_id': 'CPIAUCSL',
        'units': 'Index 1982-84=100',
        'frequency': 'Monthly',
        'description': 'Consumer Price Index for All Urban Consumers: All Items.',
        'source': 'FRED',
    },
    'Unemployment Rate': {
        'series_id': 'UNRATE',
        'units': 'Percent',
        'frequency': 'Monthly',
        'description': 'Civilian unemployment rate, seasonally adjusted.',
        'source': 'FRED',
    },
    'Fed Funds Rate': {
        'series_id': 'FEDFUNDS',
        'units': 'Percent',
        'frequency': 'Monthly',
        'description': 'Effective Federal Funds Rate.',
        'source': 'FRED',
    },
    'GDP': {
        'series_id': 'GDP',
        'units': 'Billions of Dollars',
        'frequency': 'Quarterly',
        'description': 'Gross Domestic Product.',
        'source': 'FRED',
    },
    'PCE Price Index': {
        'series_id': 'PCEPI',
        'units': 'Index 2017=100',
        'frequency': 'Monthly',
        'description': 'Personal Consumption Expenditures: Chain-type Price Index.',
        'source': 'FRED',
    },
    'Consumer Sentiment': {
        'series_id': 'UMCSENT',
        'units': 'Index 1966:Q1=100',
        'frequency': 'Monthly',
        'description': 'University of Michigan: Consumer Sentiment.',
        'source': 'FRED',
    },
}


def indicator_meta(name, default=None):
    """Return the display-metadata dict for `name`, never raising ``KeyError``.

    This is the totality core that the other accessors delegate to (mirroring
    ``categories.categorize``'s ``.get(key, default)`` one-liner):

      * a KNOWN `name`   -> its ``INDICATOR_META`` entry.
      * an UNKNOWN `name` -> the caller-supplied `default` if it is not ``None``,
        otherwise a FRESH copy of ``DEFAULT_META``.

    The unknown branch returns ``dict(DEFAULT_META)`` -- a fresh copy -- so a caller
    that mutates the result can NEVER corrupt the module-level ``DEFAULT_META``
    constant (or any later call's fallback). The returned dict always has the full
    key set, so callers may index any key (units / description / series_id / ...)
    without guarding for a missing key.
    """
    if name in INDICATOR_META:
        return INDICATOR_META[name]
    if default is not None:
        return default
    return dict(DEFAULT_META)


# Alias so the earlier spec's accessor name still imports. ``meta_for`` is exactly
# ``indicator_meta`` (same totality + fresh-copy + default semantics).
meta_for = indicator_meta


def units_of(name):
    """Return the display units string for `name` (always NON-EMPTY).

    A known indicator yields its real units (e.g. 'Percent'); an unknown one yields
    ``DEFAULT_META['units']`` ('Value'). Safe to drop straight into a plot axis title.
    """
    return indicator_meta(name)['units']


def describe(name):
    """Return the human-readable description for `name`.

    A known indicator yields its real description; an unknown one yields
    ``DEFAULT_META['description']`` ('No description available.'). Never raises.
    """
    return indicator_meta(name)['description']


# YOUR TURN: wire this metadata into the Dash UI (keep it comment-only so a plain
# ``import metadata`` stays import-pure -- no dash / plotly must enter sys.modules):
#   * axis titles / hovertemplate units -- e.g. on each figure set
#       fig.update_yaxes(title_text=units_of(name))
#     (or yaxis_title=units_of(name)) and append the units to the hovertemplate so a
#     CPI trace reads "Index 1982-84=100" while a rate trace reads "Percent".
#   * a tooltip / caption / dcc.Markdown label under each chart showing
#       describe(name)  (and optionally indicator_meta(name)['frequency'] +
#       ['series_id'] as a "Source: FRED <series_id>, Monthly" line).
#   * CSV-export headers carrying units -- e.g. when export.py builds the download,
#     title the value column f"{name} ({units_of(name)})" so the exported file
#     self-documents its units. indicator_meta() degrades to DEFAULT_META for any
#     not-yet-described indicator, so none of these wirings can KeyError.
