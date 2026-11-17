# FYP Temporal Attribution System

Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

## Overview

A streaming system built to demonstrate **how-much provenance** through **temporal attribution**
(TIN-based), the paper's design being **backward attribution** (Where-From queries). What it
actually demonstrates today is narrower, and the gap is spelled out below rather than glossed.

What exists today is the pipeline and the API around it: a Flink job that reads a tweet csv, extracts
hashtags, attributes every hashtag over event-time tumbling windows, and stores each firing window with
the per-user counts that produced it. A Spring Boot HTTP API runs the job on a stored dataset and reads
the results back, so nothing has to open the database file to show a run. See `backend/docs/API.md` for
the endpoint contract and `backend/README.md` for the reasoning behind it.

That is the how-much half, and only the half. **The backward-attribution traversal is not implemented.**
`window_outputs.vertex_id` is the constant `W1`, because there is no TIN vertex for
`WindowOutputRepository` to point at, and the four `tin_*` tables that `schema.sql` creates at startup
are created but never written to. Kafka replay and the dashboard are still planned too.

### Core Question
> "Which tweets/users caused this hashtag to trend?"

What the system answers today is the counts behind that question -- who contributed how much to one
firing window. The Where-From traversal over a temporal interaction network is the target, not the
current behaviour.

### Architecture

```
csv dataset -> Flink (windows + attribution) -> DuckDB (window_outputs) -> HTTP API -> client
                    ^                                    ^
             planned: Kafka replay        tin_* tables exist and stay empty: the backward
                                          attribution that fills them is not built yet,
                                          and neither is the dashboard
```

## Project Structure

```
FYP-stream-processing/
|
|-- README.md                           # this file
|-- Makefile                            # forwards to backend\run.bat; see the note at its top
|-- requirements.txt                    # Python deps: requests, for the API client only
|-- .gitignore
|-- .env.example                        # stale: Python-era keys (Streamlit, Kafka), nothing reads them
|-- .vscode/                            # launch.json, tasks.json, settings.json for Java + Python
|-- backend/                            # the whole system: Java, Flink, DuckDB, Spring Boot
|   |-- pom.xml                         # Java dependencies and versions
|   |-- run.bat                         # compile | serve | test | clean | reset
|   |-- README.md                       # why it is built this way, version coupling, gotchas
|   |-- docs/API.md                     # the HTTP contract clients code against
|   |-- src/main/java/com/fyp/          # api/ service/ pipeline/ storage/ config/ model/
|   |-- src/main/resources/             # config.yaml, application.properties, schema.sql
|   +-- src/test/java/com/fyp/          # 108 tests
|-- data/                               # a clone holds exactly one file, data/uploads/.gitkeep: every
|                                       # csv, parquet, the duckdb file and cleared-* archives are ignored
|-- docker/                             # Kafka + Kafka UI for the replay step (not wired yet)
|-- frontend/
|   +-- test.py                         # Python API client and smoke test
|-- scripts/data/
|   +-- generate_tweet_data.py          # makes the csv the pipeline reads; stdlib only
+-- related-work/                       # reference papers
```

## Setup

### Prerequisites
- **JDK 17** at `C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot` (the path `run.bat` sets;
  edit line 23 there if yours differs). Boot 4.1.2 runs on 17, and Flink 2.2.1 needs the `--add-opens`
  flags `run.bat` already passes.
- **Maven** -- you do not need it on `PATH`. `run.bat` finds the wrapper distribution under
  `%USERPROFILE%\.m2\wrapper\dists\apache-maven-3.9.9\` and downloads the jars from Maven Central on
  first build (about 85 jars, 171 MB, into `%USERPROFILE%\.m2\repository`).
- **Python 3.10** only if you want to run the API client, and only for `requests`. The repository does
  not carry the environment: `uv venv --python 3.10 .venv` (or `py -3.10 -m venv .venv`), then
  `pip install -r requirements.txt`. The `.venv` this was developed against is 3.10.22.
- **Docker** only for the Kafka stack, which nothing reads from yet.

### Installation

```cmd
git clone https://github.com/kosyfakicse/FYP-stream-processing.git
cd FYP-stream-processing
backend\run.bat test
```

That is the whole check: it compiles and runs 108 tests, and needs no database, no dataset, no Kafka and
no cluster -- every test builds its own storage under a temporary directory. Expected tail of the output:

```
[INFO] Tests run: 108, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

### Supply a dataset

A clone contains no csv: `data\raw\*.csv` is gitignored, so nothing on disk matches
`dataset.tweet_hashtag.file` in `backend/src/main/resources/config.yaml`. Generate one:

```cmd
python scripts/data/generate_tweet_data.py --dry-run
python scripts/data/generate_tweet_data.py --records 500 --end "2026-11-05 10:30:00"
```

That script is one stdlib-only file -- `scripts/data/generate_tweet_data.py`, deleted by `fcc44ef` along
with the `configs/tweets_config.json` it used to read, with the config folded into flags and constants.
`RECORDS`, `START`, `END` and `WINDOW_SIZE_SECONDS` at the top of the file are the row count, the
event-time range and the tumbler: 50 rows, 10:00:00 to 10:05:00 local with `END` exclusive, 60s windows.
`--records` / `--start` / `--end` / `--window-size-seconds` override them without editing the file, and
the size has to keep matching `pipeline.window_size_seconds` in config.yaml or the boundaries the script
reports are not the windows the run fires.

It also prints, for every firing window, the `outputCount` and the per-category counts the run should
store -- the known answer to check against `GET /runs/{runId}`. It writes `data\raw\tweet_hashtag.csv`,
the path config.yaml already names, and refuses to replace an existing file unless you pass `--force`,
because nothing in git can bring that upload back.

Two rules it enforces come from the Java reader rather than from csv practice: rows are sorted by
timestamp, because `DatasetScan` refuses an unsorted upload instead of letting monotonic watermarks drop
events quietly, and no text may contain a comma, because `CsvDatasetLoader` splits on the first two
commas and does no quote handling.

Or skip the file. `python -m frontend.test` uploads a five-row csv it builds in memory, so the HTTP
route needs nothing on disk at all.

```
user_id,text,timestamp
user_1,#DATA is awesome,1793844033600
user_3,#AI is awesome,1793844033601
user_2,#PYTHON is awesome,1793844033602
user_1,#AI is awesome,1793844033603
user_2,#AI is awesome,1793844033604
```

That is what the client sends, and what a hand-written file has to look like: header exactly
`user_id,text,timestamp`, one hashtag per row at minimum, timestamps in non-decreasing order.

## Usage

### Start the API and run a dataset through it

```cmd
backend\run.bat serve
```

The port is `server.port` in `backend/src/main/resources/application.properties` (8000 today). The four
calls below are the whole route, and each id is whatever the call before it printed, so substitute it
rather than copying the placeholder:

```cmd
curl -X POST -H "Content-Type: text/csv" --data-binary @data\raw\tweet_hashtag.csv "http://localhost:8000/datasets?name=tweet_hashtag.csv"
curl http://localhost:8000/datasets
curl -X POST -H "Content-Type: application/json" -d "{\"datasetId\":\"ds-...\"}" http://localhost:8000/runs
curl http://localhost:8000/runs/run-...
```

This route wants a csv on disk to upload, so it needs **Supply a dataset** above; the Python client does
not, because it builds its own bytes in memory. An unsorted file is refused at upload by `DatasetScan`,
before anything is stored: the windows are event-time with a monotonic watermark, so unsorted rows would
answer 200 and silently drop every event that looks late rather than failing the run. Request and
response bodies are in `backend/docs/API.md`.

### Run the pipeline without HTTP

```cmd
backend\run.bat
```

Uses `dataset.target_dataset` from `backend/src/main/resources/config.yaml` and writes the same tables.
This is the route that needs the csv from **Supply a dataset** above; with no file at that path it fails
at startup.

### Exercise the API from Python

```cmd
.venv\Scripts\activate
python -m frontend.test
```

### Clear the development data

```cmd
backend\run.bat reset
```

Moves the database and every uploaded csv to `data\cleared-<when>\` rather than deleting them, and
refuses while a server is listening. The next `serve` recreates the seven tables before it accepts a
request, so `GET /runs` on a cleared database answers `[]`.

## Known-Answer Test

The five-tweet fixture in `frontend/test.py` is the smallest case that shows how-much provenance:

```
Input, one 1-minute window:
  user_1: "#DATA is awesome"
  user_3: "#AI is awesome"
  user_2: "#PYTHON is awesome"
  user_1: "#AI is awesome"
  user_2: "#AI is awesome"

Output, as `GET /runs/{runId}` stores it (the extractor upper-cases and drops the `#`):
  AI      outputCount 3   attribution {user_1: 1, user_2: 1, user_3: 1}
  DATA    outputCount 1   attribution {user_1: 1}
  PYTHON  outputCount 1   attribution {user_2: 1}
```

Every hashtag is keyed separately, so no filtering step is needed, and the attribution counts sum to the
`outputCount` of the window. `backend/docs/API.md` documents the JSON this comes back as, and
`scripts/data/generate_tweet_data.py` prints the same three numbers for whatever file it generates, so a
larger dataset has a known answer to check too.

## Datasets

| Tier | Dataset | Purpose | Size | Timeline |
|------|---------|---------|------|----------|
| v1 | Tweet/Hashtag | Demo + Correctness | ~50 tweets | Weeks 3-8 |
| v2 | NYC Taxi | Real-world validation | ~10M rows | Weeks 11-12 |
| v3 | Nexmark benchmark | Performance evaluation | Configurable | Weeks 11-12 |

## Work

Student A (Vincent): Java backend

StudenT B (Leo): Python Frontend

## Development

- `backend/README.md` is where the constraints live: the Flink 2.x breaking changes, the version
  coupling in `pom.xml`, why the sink is hand-written rather than `flink-connector-jdbc`, and the
  rule that `api/` is the only package allowed to mention Spring.
- `backend/docs/API.md` is the contract. If you change an endpoint, both that file and
  `frontend/test.py` need to move with it.
- The Java tests are the only test suite, and `frontend/test.py` is a smoke test against a live server,
  not part of it. There is no lint or format step configured; the compiler level is fixed at release 17
  with `-parameters`.

## References

- **Paper:** Kosyfaki, C., Zhang, R., Mamoulis, N., & Zhou, X. (2026). Toward temporal attribution analytics in dataflows. PVLDB 19(10), 2831-2838.
- **Related Work:** See `related-work/` directory for reference papers

## License

MIT
