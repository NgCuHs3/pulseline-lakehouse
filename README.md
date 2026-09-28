# Data engineering assignment: quote lines into the lakehouse

Build a small incremental pipeline that turns batches of CRM quote data into an
Iceberg table. Plan for 2 to 3 hours. Everything runs on your laptop; there is no
Docker and no cloud account.

## Setup

You need [uv](https://docs.astral.sh/uv/) and Python 3.12.

```bash
uv sync
uv run pytest
uv run pipeline run --incoming data/incoming --warehouse warehouse
```

The stack is PyIceberg with a SQLite-backed SQL catalog, a local file warehouse,
and DuckDB. The versions are pinned in `pyproject.toml`; please keep them.

## The data

A CRM system exports a batch whenever something changes. Each batch is a directory
under `data/incoming/` with two Parquet files:

`quotes.parquet`: one row per version of a quote header.

| Column | Type | Notes |
| --- | --- | --- |
| `quote_id` | string | Stable identifier |
| `quote_number` | string | Human-facing number |
| `customer_id` | string | |
| `status` | string | `draft`, `sent`, `accepted`, `rejected` |
| `quote_date` | date | |
| `utm_source`, `utm_campaign` | string | Marketing attribution; may be null |
| `updated_at` | timestamp (UTC) | When this version was written in the CRM |

`quote_lines.parquet`: one row per version of a quote line.

| Column | Type | Notes |
| --- | --- | --- |
| `quote_line_id` | string | Stable identifier |
| `quote_id` | string | The quote this line belongs to |
| `product_sku` | string | |
| `quantity` | int32 | |
| `unit_price_vnd`, `line_total_vnd` | int64 | Whole Vietnamese dong |
| `is_deleted` | bool | The CRM deletes softly |
| `updated_at` | timestamp (UTC) | When this version was written in the CRM |

Batches arrive over time and are named so that sorting by name gives arrival order.
The export is an upstream system we do not control: treat it with the suspicion you
would treat any real source. `data/reference/campaigns.parquet` is only needed for
the stretch goal.

## What to build

Implement `uv run pipeline run --incoming <dir> --warehouse <dir>`. It is run after
every batch arrives, and each time it processes every batch in `--incoming` that it
has not processed yet.

Structure it the way our lakehouse is structured:

1. **Landing:** load the batch files verbatim into landing Iceberg tables,
   incrementally.
2. **Projection:** one SQL query, run in DuckDB, that shapes landed data into the
   target object.
3. **Object:** merge the result into the Iceberg table `crm.quote_line`, keyed on
   `quote_line_id`.

### Contract

These are fixed, because our own tests run against them:

- The CLI command and its two options, as above.
- The catalog: always open it with `pipeline.catalog.open_catalog(warehouse)`.
- The table `crm.quote_line`, whose schema is `pipeline.schema.QUOTE_LINE_SCHEMA`.
  Each row carries the line's fields plus its quote's header fields.

Everything else, including other tables you create, is your choice.

### Requirements

- `crm.quote_line` holds one row per live quote line. Each row reflects the latest
  known version of the line and of its quote.
- Running the pipeline again, or receiving the same batch twice, changes nothing.
- Lines deleted in the CRM are removed from `crm.quote_line`.
- No row is lost because it arrived late or out of order.
- Your own tests cover the behaviour you claim.

### Stretch goal (optional)

Attribute each quote to a campaign in `data/reference/campaigns.parquet` through
its UTM parameters. There is no penalty for skipping it.

## What to hand in

- Your code and tests.
- A `SOLUTION.md` of about one page covering:
    - every data problem you found in the batches, how you handled it and why
    - the decisions and trade-offs you made
    - what you would do next with more time
    - how long you spent, and which AI tools you used and where

AI coding tools are welcome. We care about the judgment calls, so explain them in
your own words.

## Submitting

1. Create a private repository from this template.
2. Commit as you go; we read the history.
