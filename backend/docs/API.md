# HTTP API Reference

The seven endpoints the backend serves, field by field. For why the surface looks like this -- the
layering, why uploads take bytes and runs take an id, how to run the pipeline from a jar -- see
[`../README.md`](../README.md). For the client-side summary and the `requests` calls, see
[`../../frontend/backend_api_reference.md`](../../frontend/backend_api_reference.md);
[`../../frontend/backend_test_script.py`](../../frontend/backend_test_script.py) drives all seven
endpoints against a live server and checks the shapes documented here, so a change to this file can be
verified by running it.

Start the server first; every example assumes it is listening:

```cmd
cd backend
run.bat serve
```

Base URL `http://localhost:8081` (`server.port` in `src/main/resources/application.properties`; 8081
because Kafka UI takes 8080 in `docker/docker-compose.yml`). There is no context path, so
`/runs` and `/datasets` hang off the host directly.

## Conventions

| Topic | Rule |
|-------|------|
| Request bodies | `POST /datasets` takes raw csv bytes (`Content-Type: text/csv` or `application/octet-stream`), never a form. `POST /runs` takes a JSON object naming a stored upload. Nothing else has a body. |
| Response bodies | JSON. Field names are camelCase (`datasetId`, `sizeBytes`), not snake_case -- the snake_case in `runs.config_json` is the config snapshot, a different thing. |
| Timestamps | ISO-8601 UTC with a trailing `Z` for the metadata fields: `2026-11-16T06:46:00Z`. Sub-second digits appear when the stored value has them, so parse them rather than compare strings. The window boundaries (`windowStart`, `windowEnd`) are epoch **milliseconds** as numbers, because that is the unit they are stored in. |
| Collections | `GET /runs` and `GET /datasets` are bare JSON arrays, not objects with a list inside. Empty list, not `null` and not a 404, when there is nothing. |
| Errors | `{"error": "message"}` on every 4xx/5xx this API answers. Two cases are refused by Spring before a controller runs -- a wrong `Content-Type` and a body that is not parseable JSON -- and those carry Spring's own shape, noted where they happen. |
| Ids | `run-20261116-0646-236e` and `ds-20261116-0646-236e`: UTC date, UTC minute, four lowercase hex digits. Regex if a client wants to validate: `^(run\|ds)-\d{8}-\d{4}-[0-9a-f]{4}$`. |
| Auth | None. Nothing in `api/` inspects a caller or a token. |
| CORS | Not configured, so a browser `fetch` from another origin is blocked. Python (`requests`, Streamlit) and curl are unaffected. |
| Idempotency | No. `POST /runs` twice is two runs with two ids; `POST /datasets` twice is two stored files. |

Anything that is not one of the seven paths below is answered by Spring Boot's default error handler,
not by this API's one: `404 {"timestamp":"...","status":404,"error":"Not Found","path":"/..."}`. Both
shapes carry an `error` key, but it holds a message where this API answered and only the status name
(`"Unsupported Media Type"`) where Spring did, so branch on `status` and treat `error` as text to show.

## Endpoints

| Method | Path | Success | Does |
|--------|------|---------|------|
| `POST` | `/runs` | `200` + `{"runId": ...}` | Runs the pipeline over a stored upload named by `datasetId`, and stores every window under a new run id. |
| `GET` | `/runs` | `200` + array | Lists every run the database holds, newest first. |
| `GET` | `/runs/{runId}` | `200` + object | One run's own row plus every window it fired. |
| `POST` | `/datasets` | `201` + `{"datasetId": ...}` | Checks and keeps the upload for later runs. |
| `GET` | `/datasets` | `200` + array | Lists every stored upload, newest first. |
| `GET` | `/datasets/{datasetId}` | `200` + object | Reads one upload's metadata. |
| `DELETE` | `/datasets/{datasetId}` | `204`, empty | Forgets an upload: its row and its file. |

---

## `POST /runs`

Runs the whole pipeline over an upload that is already on disk, and waits for it.

| Part | Detail |
|------|--------|
| Body | `{"datasetId": "ds-20261116-0646-236e"}`. Required. |
| `Content-Type` | `application/json`. |
| Query params | None. |
| Returns | `{"runId": "run-20261116-0646-236e"}` |
| Side effects | One `runs` row (`started_at`, `finished_at`, `dataset`, `config_json`) whose `dataset` column is the id you sent, and one `window_outputs` row per firing window, all stamped with the new run id. The csv is only read, so the same dataset can be run again and again. |
| Duration | Synchronous. 50 rows is about 2 s, dominated by Flink MiniCluster startup, not by your data. |

```cmd
curl -X POST -H "Content-Type: application/json" -d "{\"datasetId\":\"ds-20261116-0646-236e\"}" http://localhost:8081/runs
```

```python
import requests
# open() paths below are relative to backend/
csv = open("../data/raw/tweet_hashtag.csv", "rb").read()
dataset_id = requests.post("http://localhost:8081/datasets", params={"name": "october.csv"},
                           data=csv, headers={"Content-Type": "text/csv"}).json()["datasetId"]
run_id = requests.post("http://localhost:8081/runs", json={"datasetId": dataset_id}).json()["runId"]
```

| Status | Body | When |
|--------|------|------|
| `200` | `{"runId": "..."}` | Every window was attributed and stored. Results are already queryable. |
| `400` | `{"error": "expected {\"datasetId\": \"ds-...\"} from POST /datasets"}` | No body, or a `datasetId` that is missing or blank. |
| `404` | `{"error": "no dataset with id <datasetId>"}` | Nothing was uploaded under that id, **or** the row is there and its file is not -- a hand-cleaned uploads directory. No run row is written either way. |
| `400` | `{"error": "<csv complaint>"}` | The stored file is not a valid tweets csv any more -- see the messages below. |
| `500` | `{"error": "<ExceptionName>: <message>"}` | The data was fine and storage or the job failed. The DuckDB lock, a full disk and a Flink failure all arrive here; the class and text are the root cause's. |
| `415` | Spring's shape | Any other content type, which is what a client still sending csv bytes hits: `{"timestamp":"...","status":415,"error":"Unsupported Media Type","path":"/runs"}`. So is a body that is not parseable JSON, as a `400`. |

The 400 messages come from `CsvDatasetLoader` and name what it tripped on:

```
Expected header user_id,text,timestamp but found: <the first line>
Expected user_id,text,timestamp at line <n> but got: <the line>
Timestamp is not a long at line <n>: <the line>
```

Three things that are easy to get wrong:

- **The csv is not posted here, and that is the point.** `POST /datasets` runs `DatasetScan`, which is
  the check that the file is sorted by timestamp and has at least one row. The pipeline reads event
  time with monotonic watermarks, so unsorted input answers `200` and silently drops every event that
  looks late -- which a run can no longer be given, because it only accepts an id that passed. The
  command line job (`run.bat`) still reads `dataset.target_dataset` straight off disk with no scan.
- **A failed run still leaves a `runs` row.** The row is inserted before the csv is read, so a 400 or
  500 leaves one row with `finished_at` NULL and no `window_outputs` behind it. `ApiRoundTripTest`
  asserts exactly that, so it is the documented behaviour, not a leak to work around -- but a client
  counting runs should expect unfinished ones.
- **A 500 is not a client error and a 400 is not retryable as-is.** The 500 message is the root
  cause's class and text, so the frontend can show it without guessing; the stack trace stays in
  `logs/backend.log`.

---

## `GET /runs`

Every run the database holds as a bare JSON array, newest first (`ORDER BY started_at DESC, run_id
DESC`). Empty array before anything has run.

```cmd
curl http://localhost:8081/runs
```

```json
[{"runId":"run-20261116-0646-236e","startedAt":"2026-11-16T06:46:00Z","finishedAt":"2026-11-16T06:46:02Z","dataset":"ds-20261116-0646-236e"}]
```

| Status | Body | When |
|--------|------|------|
| `200` | the array, possibly empty | Always, as long as the database answered. |
| `500` | `{"error": "<ExceptionName>: <message>"}` | Reading `runs` failed. |

- **`dataset` is the id `POST /runs` was sent**, which is why filtering this list by dataset is one
  where clause and not a new feature. That filter does not exist yet -- the list is unfiltered, and
  runs made by the command line job carry `dataset.target_dataset` from `config.yaml` here instead
  because there was no upload to name.
- **`finishedAt` is `null` for a run that did not finish**, and the run row is written before the csv is
  even read. So this is a list of attempts, not of runs that have results: a picker built on it should
  read `finishedAt` before offering the id, or be ready to render a run with zero windows.

## `GET /runs/{runId}`

One run and everything it produced. The `windows` array is the answer set a dashboard draws from, so
this is the read that replaces the direct DuckDB query in
[Reading results without the API](#reading-results-without-the-api).

```cmd
curl http://localhost:8081/runs/run-20261116-0646-236e
```

```json
{
  "runId": "run-20261116-0646-236e",
  "startedAt": "2026-11-16T06:46:00Z",
  "finishedAt": "2026-11-16T06:46:02Z",
  "dataset": "ds-20261116-0646-236e",
  "windows": [
    {"windowStart": 1793844000000, "windowEnd": 1793844060000, "category": "JAVA", "outputCount": 1, "attribution": {"user_3": 1}},
    {"windowStart": 1793844000000, "windowEnd": 1793844060000, "category": "PYTHON", "outputCount": 2, "attribution": {"user_1": 1, "user_2": 1}}
  ]
}
```

| Status | Body | When |
|--------|------|------|
| `200` | the object above | The `runs` row exists. |
| `404` | `{"error": "no run with id <runId>"}` | Nothing ran under that id. Not used for a run with no windows. |
| `500` | `{"error": "<ExceptionName>: <message>"}` | The run row is there and reading it or its windows failed. |

| Field | Notes |
|-------|-------|
| `runId` `startedAt` `finishedAt` `dataset` | the `runs` row, same four fields as `GET /runs`. `finishedAt` is `null` while the run is going, and forever if it failed. |
| `windows` | one entry per firing window, ordered by `window_start` then `category`. Empty for a run that stored nothing. |
| `windowStart` `windowEnd` | epoch **milliseconds**, the same unit the csv timestamps are in. The window is `[start, end)`. |
| `category` | the hashtag this window was keyed by, uppercased and without the `#`: `PYTHON`. Two hashtags in one window are two entries. |
| `outputCount` | events counted in this window for this hashtag. It is the sum of `attribution`. |
| `attribution` | `attribution_json` parsed into an object: source user id to how many of those events came from them. Never `null`; `{}` cannot happen, because a window with no events does not fire. |

Two behaviours that are easy to misread:

- **A run that failed reads back as `200` with `"windows": []` and `"finishedAt": null`.** The row
  exists, so this endpoint answers it rather than calling it absent -- 404 is only for an id that never
  ran. That is the pair to the `POST /runs` note about a 400 or 500 leaving a row behind.
- **Reads wait behind runs.** One `AttributionService` means one DuckDB connection, and one connection
  cannot serve two statements, so a `GET` that arrives while a `POST` is mid-batch blocks until the job
  finishes rather than interleaving with it. At two seconds a run, that is not a wait a client notices.

---

## `POST /datasets`

Keeps an upload. `DatasetScan` inspects the bytes first, and only if they pass are they written
verbatim and recorded.

| Part | Detail |
|------|--------|
| Body | The csv, raw. Required. |
| `Content-Type` | `text/csv` or `application/octet-stream`. |
| Query param | `name` (optional) -- the label to store, see below. |
| Returns | `201` with `Location: /datasets/{datasetId}` and the object below. |
| Side effects | `datasets` row, plus the bytes at `dataset.uploads_dir/<datasetId>.csv`. Nothing is attributed. |
| Duration | Milliseconds, plus upload size -- one pass over the text and one file write. |

```cmd
curl -X POST -H "Content-Type: text/csv" --data-binary @..\data\raw\tweet_hashtag.csv "http://localhost:8081/datasets?name=october.csv"
```

```python
resp = requests.post("http://localhost:8081/datasets", params={"name": "october.csv"},
                     data=csv, headers={"Content-Type": "text/csv"})
dataset_id = resp.json()["datasetId"]        # resp.status_code == 201
```

**`name` is a label, not a path.** It lands in `original_name` only. Directories are stripped, so
`../../desktop/tweets.csv` is stored as `tweets.csv`; omit it or send it blank and the stored name
becomes `<datasetId>.csv`. The file on disk is always named from the generated id, never from the
client.

| Status | Body | When |
|--------|------|------|
| `201` | the object below | Stored. `rowCount` is the number of tweets a run will read. |
| `400` | `{"error": "empty body: POST the dataset csv bytes"}` | No bytes. |
| `400` | `{"error": "<scan complaint>"}` | Rejected. **No row and no file are left behind.** |
| `415` | `{"error": "send the raw csv bytes with Content-Type: text/csv or application/octet-stream"}` | Body sent as `application/x-www-form-urlencoded` or `multipart/form-data`. |
| `500` | `{"error": "<ExceptionName>: <message>"}` | The scan passed and the write failed -- disk full, DuckDB lock, bad insert. |

Rejection messages, from `DatasetScan`:

```
Expected header user_id,text,timestamp but found: <first line>
Expected user_id,text,timestamp at line <n> but got: <line>
Timestamp is not a long at line <n>: <line>
Dataset has no rows, only the header
Dataset is not sorted by timestamp at line <n>: <ts> comes after <previous ts>
```

The ordering check is not politeness. Line numbers are 1-based and count the header, so the first data
row is line 2.

---

## `GET /datasets`

Every stored upload as a bare JSON array, newest first (`ORDER BY uploaded_at DESC`). Empty array when
nothing has been uploaded.

```cmd
curl http://localhost:8081/datasets
```

```json
[{"datasetId":"ds-20261116-0646-236e","originalName":"october.csv","uploadedAt":"2026-11-16T06:46:00Z","expiresAt":null,"sizeBytes":1957,"rowCount":50}]
```

This is the list to render a picker from: it carries the label, the date and the row count, so one call
draws a row without fetching each dataset by id.

## `GET /datasets/{datasetId}`

One object, same shape. `404` with `{"error": "no dataset with id <datasetId>"}` for an unknown id.
The file being gone from disk while the row remains does not make this a 404 -- it makes
`DatasetService.pathOf` empty, so the metadata reads fine and `POST /runs` for that id answers 404.

## `DELETE /datasets/{datasetId}`

Removes the row and the file. Success is `204 No Content` with an empty body, so parse the status, not
the response.

```cmd
curl -X DELETE -o /dev/null -w "%{http_code}\n" http://localhost:8081/datasets/ds-20261116-0646-236e
```

`404 {"error": "no dataset with id ..."}` covers both "never stored" and "not shaped like an id": an
id is a file name here, so a path that `DatasetRepository` refuses to resolve is answered the same way
as a wrong guess. Nothing checks whether a run already points at the dataset. Deleting one that
`runs.dataset` names leaves those run rows describing a file that is gone -- their `window_outputs` are
untouched, so `GET /runs/{runId}` keeps returning the results you already have, but `POST /runs` for
that id becomes a 404 and the run cannot be repeated.

---

## The dataset object shape

`POST /datasets`, `GET /datasets` and `GET /datasets/{id}` all return this (the run objects are up at
[`GET /runs`](#get-runs)):

| Field | Type | Notes |
|-------|------|-------|
| `datasetId` | string | `ds-uuuuMMdd-HHmm-xxxx`, also the file name on disk |
| `originalName` | string | from `?name=`, stripped of directories, or `<datasetId>.csv` |
| `uploadedAt` | string | ISO-8601 UTC, when the request was accepted |
| `expiresAt` | string or `null` | `null` means keep until deleted; otherwise `uploadedAt + dataset.upload_ttl_days` |
| `sizeBytes` | number | byte length of the body, exactly what was sent |
| `rowCount` | number | data rows the scan counted; a run reads this many tweets, at least 1 |

## The csv `POST /datasets` accepts

This is the file `POST /runs` later reads, so every rule below is enforced at upload time and a stored
id has already passed them.

```
user_id,text,timestamp
user_1,Loving #python today,1793844000000
user_2,#python is awesome,1793844001200
```

- Header must start with `user_id,text,timestamp` in that order, and the timestamp must be **last on
  the line**. A fourth column passes `requireHeader` (it only looks at the first three names) and then
  fails every data row, because the loader takes the remainder of the line as the timestamp:
  `user_1,text,1793844000000,500` is parsed as `1793844000000,500`. Three columns, nothing more.
- **No field may contain a comma.** The loader splits at the first two commas and expects the rest of
  the line to be the timestamp, so `user_1,hello, world,1793...` puts `world,1793...` where the
  timestamp belongs and fails with `Timestamp is not a long at line <n>`. Real tweet text is full of
  commas, so this is the client's job: strip them, or replace them, before uploading. Nothing is
  quoted, ever.
- `timestamp` is epoch **milliseconds** as a long. Flink reads event time in milliseconds and
  `pipeline.window_size_seconds: 60` becomes 60,000 of them, so a seconds-unit csv still runs and still
  returns 200 -- it just buckets 16 hours per window. The check is ordering and parsability, not
  plausibility, so no error will tell you.
- Blank lines are skipped and do not count toward `rowCount`.
- UTF-8.

## Reading results without the API

`GET /runs/{runId}` returns these rows over HTTP, so a dashboard no longer has to open the database to
draw itself. Reading `window_outputs` directly still works and still gives the same rows -- `run_id` is
the key -- and it is the option when the server is not running:

```python
import duckdb
con = duckdb.connect("data/attribution.duckdb", read_only=True)   # from the repo root
con.execute("""
    SELECT window_start, window_end, category, output_count, attribution_json
    FROM window_outputs WHERE run_id = ? ORDER BY window_start, category
""", [run_id]).fetchdf()
```

Two things to know first:

- **Verified:** `data/attribution.duckdb` is held by one process at a time. A second `run.bat serve`,
  or `run.bat` while the server is up, dies at startup with `File is already open in ... (PID n)`.
- **Not verified:** whether `read_only=True` can open the file while the API server holds it. There is
  no `duckdb` in this environment to try it, so test that before the dashboard relies on it. Now that
  `GET /runs/{runId}` exists, a "no" costs a HTTP call per run rather than a downtime window.

The Python `duckdb` package must be 2.0.x to match the Java driver; see the version note in the README.

## Clearing the development data

No endpoint wipes history: `DELETE /datasets/{datasetId}` forgets one upload at a time and there is no
`DELETE` for runs. For a clean slate, `run.bat reset` from `backend/` moves `../data/attribution.duckdb`
(so every `runs`, `window_outputs` and `datasets` row) and `../data/uploads/ds-*.csv` into
`../data/cleared-<when>/`, which nothing in the project reads. The next `run.bat serve` applies
`schema.sql` -- the seven tables and `window_outputs_output_id_seq`, all `IF NOT EXISTS` -- while it
starts up, so `GET /runs` and `GET /datasets` answer `[]` against the empty file. You do not have to
upload something before the first read.

It refuses to do anything while a server is listening on the port in `application.properties`. That is
not ceremony: the server holds the only open handle on the file, and Windows lets you delete an open
file, so a wipe taken from another window leaves a running server happily recording runs into a file
that vanishes when it stops. Stop it first, then reset.

## Planned, not implemented

Do not write a client against these yet:

| Intended | Status |
|----------|--------|
| `GET /runs?datasetId=` | not built. `runs.dataset` holds the `datasetId` from `POST /runs`, so the filter is one where clause over the list `GET /runs` already returns. |
| `DELETE /runs/{runId}` | not built. Runs are history, and a frontend that offers a reset today gets `run.bat reset` instead. |
| A window size per run | not built. Every run uses `pipeline.window_size_seconds` from `config.yaml`, so a sweep currently means editing the file and restarting. `runs.config_json` holds the snapshot each run actually used. |
| Refusing to delete a dataset a run names | not built. See `DELETE /datasets/{datasetId}`. |
| Expiry sweep on a schedule | `DatasetService.collectExpired()` exists; nothing calls it. With `upload_ttl_days: 0` it deletes nothing regardless. |
| Kafka source, provenance graph writes | config and DDL only. |

## Config that changes what the API does

| Key (`config.yaml`) | Effect |
|-----|--------|
| `dataset.uploads_dir` | Where `POST /datasets` writes. Relative to `backend/`. |
| `dataset.upload_ttl_days` | `expiresAt` and what `collectExpired()` removes. `0` (shipped) = permanent. |
| `dataset.target_dataset` | Which `dataset.entries` key the command line job reads, and what `runs.dataset` holds for a `run.bat` run. An HTTP run records the `datasetId` it was sent, not this. |
| `pipeline.window_size_seconds` | Window length for every run. No per-run override yet. |
| `pipeline.duckdb_file` | Where results are written; also where the `datasets` table lives. |
| `server.port` (`application.properties`) | The only Spring setting. |
