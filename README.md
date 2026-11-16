# FYP Temporal Attribution System

Demonstration and Evaluation of How-Much Provenance in Streaming Dataflows

## Overview

A streaming system that demonstrates **how-much provenance** through **temporal attribution**
(TIN-based), focused on **backward attribution** (Where-From queries).

What exists today is the pipeline and the API around it: a Flink job that reads a tweet csv, extracts
hashtags, attributes every hashtag over event-time tumbling windows, and stores each firing window with
the per-user counts that produced it. A Spring Boot HTTP API runs the job on a stored dataset and reads
the results back, so nothing has to open the database file to show a run. Kafka replay, the TIN tables
and the dashboard are still planned; see `backend/docs/API.md` for the endpoint contract and
`backend/README.md` for the reasoning behind it.

### Core Question
> "Which tweets/users caused this hashtag to trend?"

### Architecture

```
csv dataset -> Flink (windows + attribution) -> DuckDB (window_outputs) -> HTTP API -> client
                    ^                                    ^
              planned: Kafka replay                planned: TIN tables, dashboard
```

## Project Structure

```
FYP-stream-processing/
|
|-- README.md                           # this file
|-- Makefile                            # forwards to backend\run.bat; see the note at its top
|-- requirements.txt                    # Python deps: requests, for the API client only
|-- backend/                            # the whole system: Java, Flink, DuckDB, Spring Boot
|   |-- pom.xml                         # Java dependencies and versions
|   |-- run.bat                         # compile | serve | test | clean | reset
|   |-- README.md                       # why it is built this way, version coupling, gotchas
|   |-- docs/API.md                     # the HTTP contract clients code against
|   |-- src/main/java/com/fyp/          # api/ service/ pipeline/ storage/ config/ model/
|   |-- src/main/resources/             # config.yaml, application.properties, schema.sql
|   +-- src/test/java/com/fyp/          # 108 tests
|-- data/                               # gitignored: attribution.duckdb, uploads/, raw/
|-- docker/                             # Kafka + Kafka UI for the replay step (not wired yet)
|-- frontend/
|   +-- test.py                         # Python API client and smoke test
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
- **Python 3.10 + a `.venv`** only if you want to run the API client. `pip install -r requirements.txt`.
- **Docker** only for the Kafka stack, which nothing reads from yet.

### Installation

```cmd
git clone <repository-url>
cd FYP-stream-processing
backend\run.bat test
```

That is the whole check: it compiles and runs 108 tests, and needs no database, no Kafka and no
cluster. Expected tail of the output:

```
[INFO] Tests run: 108, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

## Usage

### Start the API and run a dataset through it

```cmd
backend\run.bat serve
```

The port is `server.port` in `backend/src/main/resources/application.properties` (8000 today). Upload
a csv, run it, read it back:

```cmd
curl -X POST -H "Content-Type: text/csv" --data-binary @data\raw\tweet_hashtag.csv "http://localhost:8000/datasets?name=october.csv"
curl -X POST -H "Content-Type: application/json" -d "{\"datasetId\":\"ds-20261116-0945-1a5a\"}" http://localhost:8000/runs
curl http://localhost:8000/runs/run-20261116-0946-236e
```

`data\raw\tweet_hashtag.csv` is not in git. The input must be sorted by timestamp, because the windows
are event-time and a late event is dropped silently rather than failing the run; `POST /datasets`
rejects an unsorted file for that reason.

### Run the pipeline without HTTP

```cmd
backend\run.bat
```

Uses `dataset.target_dataset` from `backend/src/main/resources/config.yaml` and writes the same tables.

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

Output:
  #AI     count 3   attribution {user_1: 1, user_2: 1, user_3: 1}
  #DATA   count 1   attribution {user_1: 1}
  #PYTHON count 1   attribution {user_2: 1}
```

Every hashtag is keyed separately, so no filtering step is needed, and the attribution counts sum to
the `outputCount` of the window. `backend/docs/API.md` documents the JSON this comes back as.

## Datasets

| Tier | Dataset | Purpose | Size | Timeline |
|------|---------|---------|------|----------|
| v1 | Tweet/Hashtag | Demo + Correctness | ~50 tweets | Weeks 3-8 |
| v2 | NYC Taxi | Real-world validation | ~10M rows | Weeks 11-12 |
| v3 | Nexmark benchmark | Performance evaluation | Configurable | Weeks 11-12 |

## Development

- `backend/README.md` is where the constraints live: the Flink 2.x breaking changes, the version
  coupling in `pom.xml`, why the sink is hand-written rather than `flink-connector-jdbc`, and the
  layering rule that keeps Spring out of `pipeline/` and `storage/`.
- `backend/docs/API.md` is the contract. If you change an endpoint, both that file and
  `frontend/test.py` need to move with it.
- The Java tests are the only test suite. There is no lint or format step configured; the compiler
  level is fixed at release 17 with `-parameters`.

## Team

- **Student A (Kalifa):** Data Pipeline & Processing
- **Student B:** Provenance, Storage & Dashboard

## References

- **Paper:** Kosyfaki, C., Zhang, R., Mamoulis, N., & Zhou, X. (2026). Toward temporal attribution analytics in dataflows. PVLDB 19(10), 2831-2838.
- **Related Work:** See `related-work/` directory for reference papers

## License

MIT
