Ingest and query API for wide-event stores.
---
# Overview

The __wide__ package provides the HTTP and gRPC handlers of a wide-event store. The ingestion API supports OTLP and NDJSON.

There is no storage engine in this module. The reference implementation is not open source at the moment. The `columnstore` [Grafana data source](https://github.com/bmarinov/getclustered-columnstore-datasource) plugin supports the query API in this module.

## Project status

Work in progress. The ingestion side is functional, and the supported aggregations and filter operators are used in the Grafana plugin.

Limitations:
- No authentication on any endpoint, use a proxy.
- OTLP metrics: only gauge and sum data points are stored, histograms and summaries are dropped (WIP).
- One aggregation window per query, no ordering, no post-aggregation filter.

The [Query](#query) section is the contract. 

A few of the API contract rules are not yet enforced or validated:
- the queried time range is half-open, currently the bounds are not being checked;
- `select` in an aggregated query and `window` in a raw query are ignored instead of rejected;
- an unknown filter `op` is accepted and matches nothing;
- a numeric filter value only matches stored values of the same kind (integer or float);
- `limit` has no effect on aggregated queries;
- NDJSON rows name the timestamp as `timestamp` 
  - zero time value returned for unwindowed aggregations

## Endpoints

| Request | Body | Response |
|---|---|---|
| `POST /events` | NDJSON, one event per line. `ts` is RFC 3339 or epoch millis, every other key becomes a field | `202` |
| `POST /v1/metrics` | OTLP metrics, JSON, gzip accepted | `202`, one event per resource and timestamp |
| `POST /v1development/profiles` | OTLP profiles, JSON, gzip accepted | `202`, one event per sample |
| gRPC `ProfilesService/Export` | OTLP profiles | same as above |
| `POST /query?from=<RFC3339>&to=<RFC3339>` | query, see below | rows, NDJSON by default, JSON array with `Accept: application/json` |
| `POST /query/json?from=<RFC3339>&to=<RFC3339>` | query (DEPRECATED) | JSON array of rows |
| `GET /health` | | `200` |

Invalid queries and `wide.ErrInvalidQuery` from the store result in `400`, other store errors in `500`.

## Query

The query model is strongly inspired by [Honeycomb's API](https://docs.honeycomb.io/api/queries/create-a-query) with some operations and terms renamed. The current implementation supports only a subset of the operations.

`POST /query` returns rows according to the `Accept` header.

A query is either *raw* (no `aggregations`) or *aggregated* when `aggregations` is defined.

| Clause | Body | raw/aggregated | Description |
|---|---|---|---|
| WHERE / AND | `filters: [{ field, op, value? }]` | both | every filter must match |
| LIMIT | `limit: number` | both | raw: maximum rows. Aggregated: maximum groups. `0` or absent: no limit |
| SELECT | `select: string[]` | raw | fields to return, empty returns all |
| AGGREGATE | `aggregations: [{ op, column }]` | aggregated | one output column per entry |
| GROUP BY | `groupBy: string[]` | aggregated | one group per distinct combination of values |
| BUCKET | `window: number` | aggregated | bucket size in nanoseconds |

### Time range

`from` and `to` are required. A query covers the events with `from <= ts < to`.

### Raw queries

One row per matching event: `ts` and the event's fields. Rows are sparse and missing fields are omitted. 
- `select` specifies the returned fields; a field missing in the event is omitted.
- `limit` caps the rows; without ordering the surviving rows are unspecified.
- `groupBy` and `window` are invalid for raw queries

### Aggregated queries

Output columns are the `groupBy` fields followed by one column per aggregation. Columns are named `COUNT` or `OP(column)`, e.g. `AVG(duration_ms)`. Aggregation values are always float.

Response: one row per group. Events without the `groupBy` field form their own group with the field omitted. 

With `window`, one row per group and bucket: `ts` is the bucket start and buckets are aligned to the Unix epoch, i.e. the event time truncated to `window`. Without `window`, rows carry no `ts`. No matching events, no rows. `select` is invalid.

Ops: 
- `AVG`, `SUM`, `MAX`, `MIN` by numeric column
- `COUNT` takes no column and counts events

Aggregating a string or bool column is invalid. Columns with no matching event have `SUM` eq `0`. AVG, MIN and MAX are omitted as they are undefined on an empty result set.

### Filters

| `op` | `value` | match when |
|---|---|---|
| `eq` | string, number or bool | the field is present and equal |
| `exists` | none | the field is present |
| `not_exists` | none | the field is absent |
| `gt`, `lt`, `gte`, `lte` | number | the field is present, numeric and compares true |

Numbers compare by value. A row without the field, or with a non-numeric value under a numeric op, does not match. Any other `op` is invalid.

### Rows

The response shape follows `Accept`: 
- `application/x-ndjson` (the default) is one object per line
- `application/json` is one array of objects. 

Both responses are streamed. `ts` is RFC 3339 with nanosecond precision, the other keys are the column names.

Invalid queries:
- `groupBy` or `window` without `aggregations`
- `select` with `aggregations`
- a duplicate `select` field
- an unknown aggregation or filter `op`
- an aggregation other than `COUNT` without a column
- a non-numeric aggregation column.

A raw query with example result rows:
```json
{
  "limit": 1000,
  "select": ["service.name", "duration_ms"],
  "filters": [{ "field": "service.name", "op": "eq", "value": "api" }]
}
```
```json
{"ts":"2026-04-12T10:00:00.120Z","service.name":"api","duration_ms":12}
```

An aggregated query:

```json
{
  "filters": [{ "field": "status", "op": "gte", "value": 500 }],
  "aggregations": [{ "op": "COUNT" }, { "op": "AVG", "column": "duration_ms" }],
  "groupBy": ["service.name"],
  "window": 60000000000
}
```
```json
{"ts":"2026-04-12T10:00:00Z","service.name":"api","COUNT":3,"AVG(duration_ms)":12.5}
```

### Roadmap

Ordering and post-aggregation operations are missing. Roughly in priority order, with Honeycomb as reference:
- having: filter on the aggregated results
- [orders](https://docs.honeycomb.io/api/queries/create-a-query#body-orders): by aggregation or `groupBy` column, enables `top N by P99` queries and gives `limit` a defined meaning
- aggregations: percentiles and count_distinct
- more filter ops: `ne`, `in`, `contains`, `starts_with` for log exploration, and OR between filters

## Implementing a store

A store serves the [Query](#query) contract. `storetest.Run` is its executable form, except for the rules listed under project status as not enforced yet.

The storage backend must implement the following two interfaces:
```go
type Receiver interface {
	Receive(ctx context.Context, e wide.Event, ack func(error)) error
}

type Querier interface {
	Query(ctx context.Context, from, to time.Time, q wide.QueryParams, sink wide.Sink) error
}
```

`Query` calls `sink.Schema` once with the output columns and `sink.Row` once per row, `nil` where a row has no value. Wrap `wide.ErrInvalidQuery` for queries the store cannot serve, e.g. `AVG` over a string column.

Run the contract suite against the implementation:
```go
func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) api.Store {
		return mystore.New(t.Context())
	})
}
```

Wiring:

```go
mux := api.NewAppMux(store)
httpSrv := api.NewServer(mux, 8080)
grpcSrv, lis, err := api.NewGRPCServer(store, 4317)
```

# Development

```sh
go vet ./...
go test -race ./...
```

CI runs also include a check for an improperly referenced store package in the dependency graph.

History before "Make the module standalone" comes from the private repo and does not build on its own.

# License

Apache-2.0, see [LICENSE](LICENSE).
