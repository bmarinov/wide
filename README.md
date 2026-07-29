Ingest and query API for wide-event stores.
---
# Overview

__wide__ provides the HTTP and gRPC handlers of a wide-event store. The ingestion API supports OTLP and NDJSON.

There is no storage engine in this module. The reference implementation is not open source at the moment. The `columnstore` [Grafana data source](https://github.com/bmarinov/getclustered-columnstore-datasource) plugin supports the query API in this module.

## Project status

Work in progress. The ingestion side is functional, and the supported aggregations and filter operators are used in the Grafana plugin. 

Limitations:
- No authentication on any endpoint, use a proxy.
- OTLP metrics: only gauge and sum data points are stored, histograms and summaries are dropped (WIP).
- One aggregation window per query, no ordering.

## Endpoints

| Request | Body | Response |
|---|---|---|
| `POST /events` | NDJSON, one event per line. `ts` is RFC 3339 or epoch millis, every other key becomes a field | `202` |
| `POST /v1/metrics` | OTLP metrics, JSON, gzip accepted | `202`, one event per resource and timestamp |
| `POST /v1development/profiles` | OTLP profiles, JSON, gzip accepted | `202`, one event per sample |
| gRPC `ProfilesService/Export` | OTLP profiles | same as above |
| `POST /query?from=<RFC3339>&to=<RFC3339>` | query, see below | NDJSON, one row per line |
| `POST /query/json?from=<RFC3339>&to=<RFC3339>` | query, see below | JSON array, rows carry `ts` |
| `GET /health` | | `200` |

Invalid queries and `wide.ErrInvalidQuery` from the store result in `400`, other store errors in `500`.

## Query

Same shape the Grafana plugin builds:

| Clause | Sent as |
|---|---|
| SELECT | `select: string[]` |
| LIMIT | `limit: number` |
| WHERE / AND | `filters: [{ field, op, value? }]` |
| AGGREGATE | `aggregations: [{ op, column }]` |
| BUCKET | `window: number` (nanoseconds) |
| GROUP BY | `groupBy: string[]` |

Filter ops: `eq`, `exists`, `not_exists`, `gt`, `lt`, `gte`, `lte`. Aggregations: `COUNT`, `AVG`, `SUM`, `MAX`, `MIN`. Output columns are named `COUNT` or e.g. `AVG(duration_ms)`.

```json
{
  "limit": 1000,
  "select": ["service.name", "duration_ms"],
  "filters": [{ "field": "service.name", "op": "eq", "value": "api" }],
  "aggregations": [{ "op": "AVG", "column": "duration_ms" }],
  "groupBy": ["service.name"],
  "window": 60000000000
}
```

## Implementing a store

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
