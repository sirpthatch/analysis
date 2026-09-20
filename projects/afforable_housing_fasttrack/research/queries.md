# Reusable SoQL queries

All against Socrata endpoints on `data.cityofnewyork.us`. Unauthenticated queries are sometimes rate-limited (429) — wait and retry once. Always wrap text-field comparisons in `upper()`; casing is inconsistent across these datasets. Keep URLs under the proxy's length limit — split long `$where`/`$group` clauses across multiple simpler calls rather than one giant query if you hit a 403 "url exceeds maximum fetchable length."

## Dataset metadata / column check (do this first for any dataset before trusting field names)

```
https://data.cityofnewyork.us/api/views/{four-by-four}.json
```

## Affordable Housing Production by Building (hg8x-zxpr)

Row count and date range:
```
https://data.cityofnewyork.us/resource/hg8x-zxpr.json?$select=min(project_start_date) as first,max(project_start_date) as last,count(*) as n
```

New-construction affordable units by community district, HPD start date after a given cutoff (proxy for "in cycle"):
```
https://data.cityofnewyork.us/resource/hg8x-zxpr.json?$select=community_board,sum(all_counted_units) as u&$where=project_start_date>'2021-06-30' AND upper(reporting_construction_type) like 'NEW%25'&$group=community_board&$order=u&$limit=30
```

Construction-type split (sanity check that it's a clean binary):
```
https://data.cityofnewyork.us/resource/hg8x-zxpr.json?$select=upper(reporting_construction_type) as t,count(*) as n&$group=t
```

To tighten the numerator toward the rule's actual test, you'd ideally also require a DOB construction permit in-cycle. This dataset doesn't carry a DOB permit-issued date directly — check whether `bin`/`bbl` can be joined against a DOB permits dataset (e.g. DOB Permit Issuance, search the catalog) to add that condition.

## Housing Database by Community District (dbdt-5s7j)

Full denominator listing (all districts, including non-CD codes to filter out):
```
https://data.cityofnewyork.us/resource/dbdt-5s7j.json?$select=commntydst,cenunits20&$order=commntydst&$limit=80
```

To approximate the rule's denominator (Census + net new units through cycle start), add relevant `comp20XX` columns for years between 2020 and the cycle start:
```
https://data.cityofnewyork.us/resource/dbdt-5s7j.json?$select=commntydst,cenunits20,comp2020,comp2021&$order=commntydst&$limit=80
```
(Note: `comp2020`/`comp2021` are full calendar-year net completions, not split by half-year, so this will overshoot slightly if the cycle started mid-2021 — flag that imprecision if used.)

## Catalog search (for finding related/joinable datasets, e.g. DOB permits)

```
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofnewyork.us&search_context=data.cityofnewyork.us&only=dataset&q={your+search+term}&limit=20
```

## General catalog housekeeping

Newly published datasets (useful for checking if DCP has posted a companion dataset for the fast-track list itself):
```
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofnewyork.us&search_context=data.cityofnewyork.us&only=dataset&order=createdAt&limit=25
```

Recently updated (check periodically as Oct 1 approaches — DCP may publish the official list as a dataset rather than just a PDF/press release):
```
https://api.us.socrata.com/api/catalog/v1?domains=data.cityofnewyork.us&search_context=data.cityofnewyork.us&only=dataset&order=updatedAt&limit=40&q=fast%20track
```

## Since the pipeline exists

These queries are now codified in `src/collect.py` and `src/socrata.py` — run
`python src/collect.py all` rather than re-typing them. What is below is what
was added on 2026-09-19 and is not yet in this file above.

### DOB permits, for the rule's second milestone

Neither DOB table is filtered citywide; both are pulled for just the BBLs/BINs
carrying affordable units, chunked to keep URLs under the proxy's length limit
(`socrata.fetch_in_chunks`).

DOB NOW approved permits — current to this week, the only permit source that
covers April–June 2026:
```
https://data.cityofnewyork.us/resource/rbx6-tga4.json?$select=job_filing_number,bbl,bin,issued_date,approved_date,work_type&$where=bbl in ('1012437503',...)
```

Legacy BIS permit issuance — jobs filed before the DOB NOW cutover. Note
`bbl` is sparse in this table, so join on `bin__`; and `issuance_date` is
**text in MM/DD/YYYY**, so a SoQL date comparison on it silently returns
nothing. Filter in pandas after pulling.
```
https://data.cityofnewyork.us/resource/ipu4-2q9a.json?$select=job__,bin__,job_type,permit_type,issuance_date&$where=bin__ in ('1091038',...)
```

Work-type vocabularies, to decide what counts as "construction work":
```
https://data.cityofnewyork.us/resource/rbx6-tga4.json?$select=upper(work_type) as t,count(*) as n&$group=t&$order=n desc
https://data.cityofnewyork.us/resource/ipu4-2q9a.json?$select=upper(job_type) as j,count(*) as n&$group=j&$order=n desc
```

### Denominator cross-check on CDTA geography (48dt-mn3z)

Same DCP pipeline, different geography — the only independent read available
on `cenunits20`:
```
https://data.cityofnewyork.us/resource/48dt-mn3z.json?$select=cdta2020,cdtaname20,cenunits20,comp2020,comp2021&$limit=80
```

### Census API — now needs a key

Unauthenticated calls return an HTML "Missing Key" page rather than a JSON
error, so this fails confusingly:
```
https://api.census.gov/data/2020/dec/pl?get=NAME,H1_001N&for=tract:*&in=state:36&in=county:061
```
Request a free key at https://api.census.gov/data/key_signup.html and pass
`&key=...`.

### Community district boundaries — mind the CRS

The denominator dataset serves geometry too, which keeps the polygons and the
housing counts keyed by the same `commntydst` values:
```
https://data.cityofnewyork.us/resource/dbdt-5s7j.geojson?$select=commntydst,the_geom&$limit=200
```

**Socrata tags this layer EPSG:4326 and then ships NY State Plane feet**
(coordinates like `913175, 120128`). Reprojecting on the declared CRS sends
every vertex to infinity and the map silently renders blank — no error, no
warning, just an empty axis. Override the tag rather than transform:

```python
if abs(shapes.total_bounds).max() > 360:
    shapes = shapes.set_crs("EPSG:2263", allow_override=True)
```
