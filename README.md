# Public Transport GTFS Adapter

A MacBoard **query adapter** that reads a GTFS Static feed and exposes stop search and next-departure queries via the `transport.schedule` interface contract.

Unlike sync adapters, this adapter does **not** push data into MacBoard's database. Widgets and settings pages query it directly through MacBoard's interface proxy endpoint.

---

## Quick start

```bash
cd adapters/public_transport_gtfs
pip install -e ../../macboard_adapter  # install SDK
pip install -e .

# Set the GTFS feed URL
export GTFS_URL=https://mkuran.pl/gtfs/warsaw.zip

uvicorn app.main:app --port 8200 --reload
```

Then in MacBoard → Settings → Adapters, click **Add Adapter** and enter `http://localhost:8200`. MacBoard will call `GET /api/adapter`, discover the `transport.schedule` interface, and show the adapter as a Query adapter.

---

## Configuration

The adapter config is stored in MacBoard as YAML and sent with each request. Fields:

| Field | Required | Default | Notes |
|---|---|---|---|
| `gtfs_url` | Yes | — | URL of the GTFS Static zip |
| `gtfs_cache_dir` | No | `/tmp/macboard_gtfs` | Local directory for the cached zip |
| `stop_ids` | No | _(all)_ | Comma-separated stop_ids to index. Use to limit memory on large feeds. |

Example:

```yaml
gtfs_url: https://mkuran.pl/gtfs/warsaw.zip
gtfs_cache_dir: /tmp/macboard_gtfs
# stop_ids: "7001,7002,7003"
```

---

## Startup pre-loading

On startup, the adapter reads the GTFS URL from environment variables and pre-loads the feed so the first query is instant:

| Variable | Notes |
|---|---|
| `GTFS_URL` | URL of the GTFS zip |
| `GTFS_CACHE_DIR` | Cache directory |
| `GTFS_STOP_IDS` | Comma-separated stop_id filter |
| `GTFS_CONFIG_YAML` | Full YAML block (overrides individual vars) |

---

## Interface endpoints

Mounted under `/api/interfaces/transport.schedule/`:

### `GET /stops/search?q=<text>[&limit=<n>]`

Search stops by name fragment.

```json
{
  "stops": [
    { "stop_id": "7001", "name": "Warszawa Centralna", "lat": 52.229, "lon": 21.003 }
  ]
}
```

### `GET /stops/{stop_id}`

Return one stop record.

### `GET /stops/{stop_id}/departures?limit=<n>[&at=<iso_datetime>]`

Return the next N departures from a stop.

```json
{
  "stop": { "stop_id": "7001", "name": "Warszawa Centralna", "lat": 52.229, "lon": 21.003 },
  "departures": [
    {
      "route_id": "KW",
      "route_short_name": "KW",
      "route_long_name": "Koleje Warszawskie",
      "trip_headsign": "Warszawa Wschodnia",
      "departure_time": "2026-04-05T14:32:00",
      "realtime": false
    }
  ]
}
```

---

## Memory notes

`stop_times.txt` in a full GTFS feed can contain millions of rows. Use the `stop_ids` config filter to limit the index to the stops you actually need. For example, to track a single commuter stop, list only that stop's `stop_id`.

The GTFS zip is cached locally and re-downloaded only when the remote server reports a changed `ETag` or `Last-Modified` header.
