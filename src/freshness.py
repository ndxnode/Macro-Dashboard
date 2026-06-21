# freshness.py
"""Per-indicator data-freshness / staleness summary.

This is a pure pandas/numpy helper with no database, network, or plotting
dependency, so it can be unit-tested against synthetic data (there is likely
no FRED_API_KEY in this environment, and kaleido/scipy are absent). Nothing
here imports sqlite3/Dash/plotly/kaleido at module top -- this mirrors the
import purity of ``src/compare.py`` / ``src/signals.py`` / ``src/corr_matrix.py``
so the module can be exercised offline.

Each indicator is expected as a tidy DataFrame with at least 'date' and 'value'
columns -- the same ``{indicator_name -> tidy {'date','value'} frame}`` dict
shape ``alerts.build_alerts_payload`` / ``corr_matrix.correlation_matrix``
already consume.

The flagship helper, ``summarize_freshness``, takes that dict and returns a
sorted-by-name ``list[dict]`` of staleness rows -- a ready-to-render badge-table
source (see the dash_table.DataTable YOUR TURN seam below). It answers the
operational question "did one indicator's GitHub-Actions cron ETL go stale
while its siblings kept updating?".
"""
import pandas as pd
import numpy as np


def _to_dated_series(df, name='value'):
    """Normalize a tidy {'date','value'} DataFrame into a named, date-indexed
    numeric Series sorted by date with duplicate dates dropped (last wins).

    A local copy of ``compare._to_dated_series`` so this module stays
    self-contained and import-pure (no cross-module import); identical body.
    """
    if df is None or df.empty or 'date' not in df.columns or 'value' not in df.columns:
        return pd.Series(dtype='float64', name=name)

    s = df[['date', 'value']].copy()
    s['date'] = pd.to_datetime(s['date'], errors='coerce')
    s['value'] = pd.to_numeric(s['value'], errors='coerce')
    s = s.dropna(subset=['date'])
    s = s.sort_values('date').drop_duplicates(subset='date', keep='last')
    return pd.Series(s['value'].values, index=s['date'].values, name=name)


def summarize_freshness(indicator_frames, as_of=None, stale_after_days=45):
    """Summarize how fresh each indicator's data is, one row per indicator.

    ``indicator_frames`` is a ``{indicator_name -> tidy {'date','value'} frame}``
    dict (the same shape ``alerts.build_alerts_payload`` /
    ``corr_matrix.correlation_matrix`` consume). Each frame is normalized through
    ``_to_dated_series`` (date-indexed, sorted, deduped, coerced); the result is
    a list of dicts -- one per indicator key, ORDERED by ``sorted(indicator_frames)``:

        {'indicator', 'last_date', 'age_days', 'n_points', 'is_stale'}

    where:
      * ``last_date``  -- the max valid date in the frame as a ``pd.Timestamp``
        (``.date()`` is available to the future Dash panel), or ``None`` for an
        empty / missing-cols / all-NaT-date frame.
      * ``age_days``   -- ``(ref - last_date).days`` (a python ``int``), or
        ``None`` when either ``ref`` or ``last_date`` is ``None``.
      * ``n_points``   -- the count of valid-DATE rows (``len`` of the normalized
        series). NaN-VALUE rows are COUNTED (freshness is about DATES); only
        NaT-date rows are dropped.
      * ``is_stale``   -- ``(age_days is not None) and (age_days > stale_after_days)``.

    Reference date (``ref``) resolution:
      * if ``as_of is not None`` -> ``pd.Timestamp(as_of)`` (coerces a str / date
        / Timestamp) -- a real "stale vs today / vs a fixed date" check.
      * else -> the SINGLE GLOBAL max of all non-None ``last_date`` values across
        EVERY frame (skipping empty / all-NaT frames). This default flags a
        lagging indicator (whose cron ETL went stale while its siblings kept
        updating) with ZERO arguments: that indicator's ``last_date`` lags the
        global max, so its ``age_days`` exceeds ``stale_after_days``. A per-frame
        own-max would make every default ``age_days == 0`` (``is_stale`` always
        ``False``) -- a no-op default -- so GLOBAL max is what serves the module's
        purpose. If ALL frames are empty (no valid ``last_date``), the global max
        is ``None`` -> every ``age_days`` is ``None``, every ``is_stale`` False,
        no crash.

    Returns ``[]`` for a falsy mapping (``None`` AND ``{}``) -- guarded BEFORE
    ``.keys()`` because ``None`` has no ``.keys()`` (mirrors
    ``corr_matrix.correlation_matrix`` / ``alerts.build_alerts_payload``).
    It NEVER raises on None / {} / empty / all-NaT / single / mixed-empty input.
    """
    # Guard a falsy mapping (None or empty dict) BEFORE touching ``.keys()`` --
    # ``None`` has no ``.keys()`` (an opaque AttributeError). Mirrors the
    # corr_matrix / alerts tolerance for the identical {indicator -> frame} shape.
    if not indicator_frames:
        return []

    names = sorted(indicator_frames.keys())

    # First pass: normalize each frame and capture its (last_date, n_points).
    last_dates = {}
    n_points = {}
    for name in names:
        s = _to_dated_series(indicator_frames[name], name=name)
        last_dates[name] = pd.Timestamp(s.index.max()) if len(s) else None
        n_points[name] = len(s)

    # Resolve the reference date.
    if as_of is not None:
        ref = pd.Timestamp(as_of)  # coerces str / date / Timestamp
    else:
        valid_lasts = [d for d in last_dates.values() if d is not None]
        ref = max(valid_lasts) if valid_lasts else None

    # Second pass: build the ordered rows.
    rows = []
    for name in names:
        last_date = last_dates[name]
        if ref is not None and last_date is not None:
            age_days = (ref - last_date).days  # python int
        else:
            age_days = None
        # GUARD: ``None > stale_after_days`` is a TypeError -- never compare a
        # None age. A None-age row (empty / all-NaT frame, or all-empty input)
        # is reported NOT stale.
        is_stale = (age_days is not None) and (age_days > stale_after_days)
        rows.append({
            'indicator': name,
            'last_date': last_date,
            'age_days': age_days,
            'n_points': n_points[name],
            'is_stale': is_stale,
        })
    return rows


# YOUR TURN: a thin Dash panel rendering the freshness rows as a badge table.
# Once the app builds the ``{indicator -> tidy frame}`` dict (the same one
# ``alerts.build_alerts_payload`` consumes), the staleness badge table is
# essentially one call -- the list[dict] is already a DataTable ``data`` payload,
# and ``style_data_conditional`` highlights the stale rows red:
#     from dash import dash_table
#     rows = summarize_freshness(frames)
#     table = dash_table.DataTable(
#         columns=[{'name': c, 'id': c} for c in
#                  ('indicator', 'last_date', 'age_days', 'n_points', 'is_stale')],
#         data=[{**r, 'last_date': (r['last_date'].date().isoformat()
#                                   if r['last_date'] is not None else '')}
#               for r in rows],
#         style_data_conditional=[{
#             'if': {'filter_query': '{is_stale} = true'},
#             'backgroundColor': '#ffe5e5', 'color': '#a00',  # red-highlight stale
#         }],
#     )
# The pure, tested part is ``summarize_freshness`` above; the plotting/wiring
# stays here as a later increment so this module is import-pure (dash must not
# enter sys.modules via a plain ``import freshness``).
