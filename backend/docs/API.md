# HTTP API Reference

The eight endpoints the backend serves, field by field. For why the surface looks like this -- the
layering, why uploads take bytes and runs take an id, how to run the pipeline from a jar -- see
[`../README.md`](../README.md). For the client side there is
[`../../frontend/test.py`](../../frontend/test.py): one function per endpoint, each returning the
`requests` response. Its `main()` walks the whole path in order -- `POST /datasets` with the five-tweet
fixture, `POST /runs` with the id that came back, then the two reads over that run id:
`GET /runs/{runId}` for the windows and `GET /runs/{runId}/graph` for the provenance, each printing the
status code and the body. It is the only caller in the repository, and it reads its own base URL from the
`BASE_URL` constant at the top -- so when the port below changes, that line has to change too. Every run
of `main()` writes a dataset and a run, so it is a smoke test with side effects, not a read-only check.

Start the server first; every example assumes it is listening:

```cmd
cd backend
run.bat serve
```

Base URL `http://localhost:8000` (`server.port` in `src/main/resources/application.properties`). There
is no context path, so `/runs` and `/datasets` hang off the host directly. The value is not compiled
in anywhere: a running server keeps the port it read at launch, so edit the file before `run.bat serve`
and not while it is already up.

## Conventions

| Topic | Rule |
|-------|------|
| Request bodies | `POST /datasets` takes raw csv bytes (`Content-Type: text/csv` or `application/octet-stream`), never a form. `POST /runs` takes a JSON object naming a stored upload. Nothing else has a body. |
| Response bodies | JSON. Field names are camelCase (`datasetId`, `sizeBytes`), not snake_case -- the snake_case in `runs.config_json` is the config snapshot, a different thing. |
| Timestamps | ISO-8601 UTC with a trailing `Z` for the metadata fields: `2026-11-16T06:46:00Z`. Sub-second digits appear when the stored value has them, so parse them rather than compare strings. The window boundaries (`windowStart`, `windowEnd`) are epoch **milliseconds** as numbers, because that is the unit they are stored in. |
| Collections | `GET /runs` and `GET /datasets` are bare JSON arrays, not objects with a list inside. Empty list, not `null` and not a 404, when there is nothing. `GET /runs/{runId}/graph` is the exception: an object holding four arrays, because its four tables have four different row shapes. |
| Errors | `{"error": "message"}` on every 4xx/5xx this API answers. Two cases are refused by Spring before a controller runs -- a wrong `Content-Type` and a body that is not parseable JSON -- and those carry Spring's own shape, noted where they happen. |
| Ids | `run-20261116-0646-236e` and `ds-20261116-0646-236e`: UTC date, UTC minute, four lowercase hex digits. Regex if a client wants to validate: `^(run\|ds)-\d{8}-\d{4}-[0-9a-f]{4}$`. |
| Auth | None. Nothing in `api/` inspects a caller or a token. |
| CORS | Not configured, so a browser `fetch` from another origin is blocked. Python (`requests`, Streamlit) and curl are unaffected. |
| Idempotency | No. `POST /runs` twice is two runs with two ids; `POST /datasets` twice is two stored files. |

Anything that is not one of the eight paths below is answered by Spring Boot's default error handler,
not by this API's one: `404 {"timestamp":"...","status":404,"error":"Not Found","path":"/..."}`. Both
shapes carry an `error` key, but it holds a message where this API answered and only the status name
(`"Unsupported Media Type"`) where Spring did, so branch on `status` and treat `error` as text to show.

## Endpoints

| Method | Path | Success | Does |
|--------|------|---------|------|
| `POST` | `/runs` | `200` + `{"runId": ...}` | Runs the pipeline over a stored upload named by `datasetId`, and stores every window under a new run id. |
| `GET` | `/runs` | `200` + array | Lists every run the database holds, newest first. |
| `GET` | `/runs/{runId}` | `200` + object | One run's own row plus every window it fired. |
| `GET` | `/runs/{runId}/graph` | `200` + object | The same run as a temporal interaction network: operators, flows, states and interactions. |
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
| Side effects | One `runs` row (`started_at`, `finished_at`, `dataset`, `config_json`, `provenance_enabled`) whose `dataset` column is the id you sent, one `window_outputs` row per firing window, and the same rows' provenance: four `tin_vertices` (`K1`, `M1`, `W1`, `S1`), three `tin_edges` (`K1->M1`, `M1->W1`, `W1->S1`), plus rows the job measures per interval -- `tin_states` holds the running box of `K1` and of `M1` once per `state_tick_seconds` and one `W1` row per fired (window, category), and `tin_interactions` holds `K1->M1` and `M1->W1` once per `interaction_tick_seconds` and one `W1->S1` row per firing. The three intervals come from `config.yaml` and currently all equal `window_size_seconds`, so today that is one row each per interval; see `docs/TIN.md` for what a finer tick changes. All of it stamped with the new run id, so a second run of the same dataset repeats the ids rather than colliding. The csv is only read, so the same dataset can be run again and again. |
| Duration | Synchronous. 50 rows is about 2 s, dominated by Flink MiniCluster startup, not by your data. |

```cmd
curl -X POST -H "Content-Type: application/json" -d "{\"datasetId\":\"ds-20261116-0646-236e\"}" http://localhost:8000/runs
```

```python
import requests
# open() paths below are relative to backend/
csv = open("../data/raw/tweet_hashtag.csv", "rb").read()
dataset_id = requests.post("http://localhost:8000/datasets", params={"name": "october.csv"},
                           data=csv, headers={"Content-Type": "text/csv"}).json()["datasetId"]
run_id = requests.post("http://localhost:8000/runs", json={"datasetId": dataset_id}).json()["runId"]
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
curl http://localhost:8000/runs
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
curl http://localhost:8000/runs/run-20261116-0646-236e
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

## `GET /runs/{runId}/graph`

The run as a temporal interaction network: the four `tin_*` tables in one body, so a graph viewer needs
no database driver. `GET /runs/{runId}` gives the same run's *results*; this gives the machinery that
produced them and where each number was taken.

```cmd
curl http://localhost:8000/runs/run-20261116-0646-236e/graph
```

```python
graph = requests.get("http://localhost:8000/runs/run-20261116-0646-236e/graph", timeout=30).json()
by_id = {v["vertexId"]: v for v in graph["vertices"]}     # look a description up while drawing
edges_from = {(e["sourceVertex"], e["targetVertex"]) for e in graph["edges"]}
```

```json
{
  "vertices": [
    {"vertexId": "K1", "vertexType": "source", "description": "csv dataset ds-20261116-0646-236e (3 rows)"},
    {"vertexId": "M1", "vertexType": "map", "description": "hashtag extractor, one event per hashtag"},
    {"vertexId": "S1", "vertexType": "sink", "description": "duckdb sink, window_outputs, tin_interactions and tin_states"},
    {"vertexId": "W1", "vertexType": "window", "description": "60s tumbling count, keyed by category"}
  ],
  "edges": [
    {"sourceVertex": "K1", "targetVertex": "M1"},
    {"sourceVertex": "M1", "targetVertex": "W1"},
    {"sourceVertex": "W1", "targetVertex": "S1"}
  ],
  "states": [
    {"vertexId": "K1", "category": null, "timeStart": 1793844000000, "timeEnd": 1793844060000, "quantity": 3, "attribution": {}, "firedAt": 1793844060000},
    {"vertexId": "M1", "category": null, "timeStart": 1793844000000, "timeEnd": 1793844060000, "quantity": 3, "attribution": {}, "firedAt": 1793844060000},
    {"vertexId": "W1", "category": "JAVA", "timeStart": 1793844000000, "timeEnd": 1793844060000, "quantity": 1, "attribution": {"user_3": 1}, "firedAt": 1793844060000},
    {"vertexId": "W1", "category": "PYTHON", "timeStart": 1793844000000, "timeEnd": 1793844060000, "quantity": 2, "attribution": {"user_1": 1, "user_2": 1}, "firedAt": 1793844060000}
  ],
  "interactions": [
    {"sourceVertex": "K1", "targetVertex": "M1", "category": null, "timestamp": 1793844060000, "quantity": 3, "attribution": {"K1": 3}},
    {"sourceVertex": "M1", "targetVertex": "W1", "category": "JAVA", "timestamp": 1793844060000, "quantity": 1, "attribution": {"M1": 1}},
    {"sourceVertex": "M1", "targetVertex": "W1", "category": "PYTHON", "timestamp": 1793844060000, "quantity": 2, "attribution": {"M1": 2}},
    {"sourceVertex": "W1", "targetVertex": "S1", "category": "JAVA", "timestamp": 1793844060000, "quantity": 1, "attribution": {"W1": 1}},
    {"sourceVertex": "W1", "targetVertex": "S1", "category": "PYTHON", "timestamp": 1793844060000, "quantity": 2, "attribution": {"W1": 2}}
  ]
}
```

| Status | Body | When |
|--------|------|------|
| `200` | the object above, with any of the four arrays empty | The `runs` row exists. |
| `404` | `{"error": "no run with id <runId>"}` | Nothing ran under that id. Checked against `runs` first, so this is the same 404 `GET /runs/{runId}` gives. |
| `500` | `{"error": "<ExceptionName>: <message>"}` | The run row is there and reading one of the four tables failed. |

| Field | Notes |
|-------|-------|
| `vertices` | the operators the run built, one row per `tin_vertices` row, `ORDER BY vertex_id`. `vertexType` is one of `source`, `map`, `filter`, `window`, `sink`; `description` is prose written by the job that assembles the topology, so its wording is not a contract -- key on `vertexId`. |
| `edges` | `tin_edges` for the run, `ORDER BY source_vertex, target_vertex`. Ids only -- a flow carries no count, the counts live in `interactions`. |
| `states` | `tin_states`, `ORDER BY time_start, vertex_id, category`. One state is what one vertex held over `[timeStart, timeEnd)`. Read it per vertex: a `W1` row is that window's answer, while a `K1` or `M1` row is **cumulative** -- what the box has held since the run started, not what arrived in that interval. |
| `interactions` | `tin_interactions`, `ORDER BY timestamp, source_vertex, target_vertex, category`. One row is what one edge carried during one interval, so `timestamp` is that interval's end -- an event-time reading, never a wall clock. The unit changes with the edge: tweets over `K1->M1`, hashtag events over `M1->W1`, results over `W1->S1`, which is exactly why neighbouring rows can disagree. |
| `runId` | nowhere. It is the path variable you sent, so no row repeats it. |
| `category` | `null` is a legal value and means "this row is not keyed by hashtag" -- a whole-vertex count. Three row kinds always answer `null`: `K1`'s box, `M1`'s box and the `K1->M1` interaction, because a tweet is not a tag. Expect it; do not treat it as missing data. |
| `timeStart` `timeEnd` `timestamp` `firedAt` | epoch **milliseconds**, as numbers, exactly like `windowStart` above. `firedAt` is the window's own close in event time, not the moment the job ran. |
| `attribution` | **two key domains, and an empty object.** On a `W1` state row it is contributor user id to how much of `quantity` they account for, exactly like `windows[].attribution` in `GET /runs/{runId}`. On an interaction it is one key, the `vertexId` that handed the data over, because that is the only origin the edge can claim. A `K1` or `M1` state row is `{}` -- `{}`, not `null`: the operator held a count and never a tally. In every case `quantity` equals the map's sum, which is the invariant a checker should assert rather than trust. |

Four things a client will hit:

- **`vertices` is sorted by id, not by dataflow.** Lexicographically that puts `S1` before `W1`, so
  drawing the chain in pipeline order means ordering by `vertexType` or walking `edges` from the
  `source` vertex -- do not assume the array is already topological.
- **Empty arrays are normal, and which ones are empty tells you how far the run got.** Both callers --
  `POST /runs` and the command line job -- go through the same service, so `vertices` and `edges` are
  written before the job executes. If `states` holds just the `K1` and `M1` boxes and `interactions` just
  the `K1->M1` interaction, the csv was read but no hashtag event ever closed a window -- a file with no
  hashtags in it, or timestamps that never pass a window end. If all four are empty, the run died while
  reading the csv, or it predates the provenance capture -- every `run-20261116-*` row answers this way.
- **`states` holds the source and map boxes as well as the windows.** `K1`'s and `M1`'s rows are what make
  the graph worth drawing: their quantities are rows read and events emitted, counted on the streams that
  carry them, so a file whose four tweets make five events shows that gap instead of hiding it. `W1`'s
  rows are still one per fired (window, category), so `states` filtered to `vertexId == "W1"` and the
  `windows` array of `GET /runs/{runId}` remain the same numbers in a different shape. Snapshotting `W1`
  mid-window needs a different operator and is not built; `docs/TIN.md` says what it would take.
- **One `K1->M1` row per interval, and its `category` is `null`.** It counts *tweets*, so a tweet tagged
  `#AI #DATA` counts once here and twice over `M1->W1`, where the stream is keyed by hashtag and each tag
  gets its own row. The pair is the difference between what was read and what was emitted, which is the
  thing an earlier capture could not see from inside the window.

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
curl -X POST -H "Content-Type: text/csv" --data-binary @..\data\raw\tweet_hashtag.csv "http://localhost:8000/datasets?name=october.csv"
```

```python
resp = requests.post("http://localhost:8000/datasets", params={"name": "october.csv"},
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
curl http://localhost:8000/datasets
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
curl -X DELETE -o /dev/null -w "%{http_code}\n" http://localhost:8000/datasets/ds-20261116-0646-236e
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
- **Verified 2026-11-22, with no `duckdb` python package installed:** the same read is possible from the
  driver the project already has, while nothing holds the file -- Java single-file source mode against
  the jar in the local repository. The property is `access_mode`, not `read_only`: a `read_only` property
  is rejected outright with `The following options were not recognized: read_only`.

  ```cmd
  java -cp %USERPROFILE%\.m2\repository\org\duckdb\duckdb_jdbc\2.0.0.1\duckdb_jdbc-2.0.0.1.jar Check.java data\attribution.duckdb
  ```

  with `Properties.setProperty("access_mode", "read_only")` passed to `DriverManager.getConnection`
  (`"jdbc:duckdb:" + path`), and `SELECT ... FROM information_schema.columns` or `duckdb_constraints()`
  for the schema. This is how the reset claim below was checked rather than assumed.

The Python `duckdb` package must be 2.0.x to match the Java driver; see the version note in the README.

## Clearing the development data

No endpoint wipes history: `DELETE /datasets/{datasetId}` forgets one upload at a time and there is no
`DELETE` for runs. For a clean slate, `run.bat reset` from `backend/` moves `../data/attribution.duckdb`
(so every `runs`, `window_outputs` and `datasets` row) and `../data/uploads/ds-*.csv` into
`../data/cleared-<when>/`, which nothing in the project reads. The next `run.bat serve` applies
`schema.sql` -- the seven tables and the three id sequences (`window_outputs_output_id_seq`,
`tin_states_state_id_seq`, `tin_interactions_interaction_id_seq`), all `IF NOT EXISTS` -- while it
starts up, so `GET /runs` and `GET /datasets` answer `[]` against the empty file. You do not have to
upload something before the first read.

Reset it after a `schema.sql` edit too, not only for a clean slate. `IF NOT EXISTS` means a table that
already exists keeps the columns it was created with, so a database written before the `tin_*` tables
were fixed still holds the old definitions and the new inserts fail on the columns they reference. The
per-interval capture is one such edit: `tin_states` and `tin_interactions` each gained a `category`
column, so a file older than it fails on its first tick row with a binder error naming that column -- and
that error is the only way to tell, short of querying `information_schema.columns` yourself. The current
`../data/attribution.duckdb` in this checkout has already been checked: the column is there, nullable,
with the two natural unique keys, so no wipe is needed before the first run of this capture.

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
| Kafka source | config only. |
| Backward attribution -- a Where-From query over the graph | not built. `GET /runs/{runId}/graph` returns the whole network and stops there; nothing walks `edges` in reverse from a window, so asking "which tweets caused this trend" is still client-side work over the four arrays. |
| `S1` state rows, and `W1` snapshots between windows | not built. `K1`'s and `M1`'s boxes are written per `state_tick_seconds` since 2026-11-22; `W1` still reports only when its window closes, because a Flink window function cannot see its own bucket mid-flight, and the sink's buffer has no recorder at all. |
| `GET /runs/{runId}/graph?category=AI` | not built. The body is the run's whole graph, unfiltered; a per-category view is a client-side filter over `states` and `interactions`. |

## Config that changes what the API does

| Key (`config.yaml`) | Effect |
|-----|--------|
| `dataset.uploads_dir` | Where `POST /datasets` writes. Relative to `backend/`. |
| `dataset.upload_ttl_days` | `expiresAt` and what `collectExpired()` removes. `0` (shipped) = permanent. |
| `dataset.target_dataset` | Which `dataset.entries` key the command line job reads, and what `runs.dataset` holds for a `run.bat` run. An HTTP run records the `datasetId` it was sent, not this. |
| `pipeline.window_size_seconds` | Window length for every run. No per-run override yet. |
| `pipeline.state_tick_seconds` | How often `K1`'s and `M1`'s boxes are written. `W1` ignores it until it can be snapshotted mid-window. |
| `pipeline.interaction_tick_seconds` | How often `K1->M1` and `M1->W1` are written. One `W1->S1` row per firing either way. |
| `pipeline.duckdb_file` | Where results are written; also where the `datasets` table lives. |
| `server.port` (`application.properties`) | The only Spring setting. |

Both ticks are checked against the window when the config is read, and must divide it: a `7` under a `60`
makes the server fail to start rather than write rows that cannot be summed. The message names both
numbers. See `docs/TIN.md`, "Intervals".
