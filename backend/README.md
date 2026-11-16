# Backend: Streaming Pipeline (Java)

Dataset -> Flink -> DuckDB pipeline with how-much provenance capture.

The pipeline is a function of its input, which is the shape the whole project is built around:

```java
String runId = service.run(csvPath);   // execute, persist, return an identifier
RunResults results = service.fetchAll(runId);   // every window result that run produced
```

`runId` is generated per execution (`run-20261114-1516-3fa4`: UTC date, UTC minute, random suffix),
recorded in the `runs` table, and stamped on every row the run writes.

The Streamlit UI lives outside this folder and reads the DuckDB file this pipeline writes.
**DuckDB is the contract between backend and frontend** -- no message bus. `POST /runs` exists to
trigger a run over HTTP and `GET /runs/{runId}` reads its windows back over it, but the frontend does
not have to go through either; it can open the file itself.

## What Is Implemented

| Path | Status |
|------|--------|
| CSV dataset -> hashtags -> event-time tumbling windows -> per-source attribution -> DuckDB | working, see `AttributionJob` |
| `runs` + `window_outputs` writes and run-scoped retrieval | working |
| `datasets` rows + the uploads directory (`DatasetRepository`) | working, `/datasets` POST, GET, GET-by-id, DELETE |
| `POST /runs` (`{"datasetId": ...}` in, run id out) over Spring Boot | working, see `run.bat serve` |
| `GET /runs` and `GET /runs/{runId}` (the run rows, and one run with every window it fired) | working |
| Kafka source | not wired; config has no kafka block yet (see `docs` in the repo root) |
| Provenance graph (`tin_vertices`, `tin_edges`, `tin_interactions`, `tin_states`) | DDL only, no reads or writes; `runs.provenance_enabled` is written as `false` |
| Streamlit dashboard | not started |

## Requirements

| Tool | Version | Why this version |
|------|---------|------------------|
| JDK | **17** (Temurin) | Flink 2.x default and recommended. Java 18/20 are unsupported. |
| Maven | 3.9.9 | `run.bat` finds it under `%USERPROFILE%\.m2\wrapper\dists\`, else uses `mvn` on `PATH` |
| Kafka | 3.x | Only needed once the Kafka stage starts |
| Flink | 2.2.1 | Pinned in `pom.xml` |
| Spring Boot | 4.1.2 | Newest non-RC; its BOM owns the Jackson, logging and test-library versions |

## First-Time Setup

Only the JDK needs a system install. `run.bat` injects `JAVA_HOME` for that one command so it never
touches your global environment. This matters here: a pre-existing JDK 20 on `PATH` otherwise
shadows the Temurin 17 build.

Run every command from inside `backend/`.

```cmd
:: 1. Install JDK 17 only (elevated terminal)
winget install EclipseAdoptium.Temurin.17.JDK

:: 2. Verify the toolchain -- MiniCluster in-process, no cluster or Kafka needed
run.bat test

:: 3. Run the pipeline on the configured dataset, then read the results back by run id
run.bat
```

Step 2 is the gate. The suite starts a real Flink MiniCluster in-process and writes to real DuckDB
files under JUnit temp directories, so all 108 tests passing means the JDK, Flink and the DuckDB driver
all line up -- and it never opens `../data/attribution.duckdb`, so it runs while the API server is up.
[Tests](#tests) says what each class covers.

`run.bat` compiles and runs through Maven rather than a hand-written `javac` line on purpose: the
compile classpath is 85 jars (171 MB) and 8,644 characters, longer than the 1,023 characters cmd.exe
will hold in a variable, so a manual command line cannot carry it. Nothing is generated into
`target/`, so `mvn clean` cannot break the script.

## Version Coupling -- Read Before Editing `pom.xml`

Flink connectors are versioned **independently** of Flink core, and the artifact version carries
the Flink version as a suffix. Using the bare version produces
`Could not find artifact org.apache.flink:flink-connector-kafka:jar:5.0.0` -- the connector
number alone does not exist.

| Flink core | `flink-connector-kafka` | `flink-connector-jdbc` |
|------------|-------------------------|------------------------|
| 1.20.x | `3.4.0-1.20` | `3.4.0-1.20` |
| 2.0.x | `4.0.1-2.0` | none released |
| 2.1.x | `5.0.0-2.1` | none released |
| **2.2.x (ours)** | **`5.0.0-2.2`** | **none released** |

`flink-connector-jdbc` has never published a Flink 2.x build -- `3.4.0-1.20` is still the latest
on Maven Central. So the DuckDB sink talks to **`org.duckdb:duckdb_jdbc` directly through a
Sink V2 implementation** rather than through the JDBC connector. That is a deliberate design
constraint, not an oversight.

DuckDB JDBC is `2.0.0.1` (note the **4-part** version, unlike most artifacts). Nothing in Python opens
the database any more -- `GET /runs/{runId}` is how results are read, and `requirements.txt` is just
`requests` for `frontend/test.py` -- but if you do want to read `../data/attribution.duckdb` yourself,
install the Python `duckdb` package at 2.0.x (`pip install "duckdb~=2.0.0"`) to match the Java driver:
a newer driver can read an older file, an older driver cannot read a newer one. Do it while no server
is running, because the file is held by one process at a time. If Python fails with a "file format
version" error, align these two.

Since the HTTP API, the Spring Boot BOM is imported and **owns the Jackson, logging and test-library
versions**, so those dependencies are declared without `<version>`. Two consequences:

- Re-adding a version to `jackson-*` forks the classpath. Boot serialises HTTP bodies with Jackson 3
  (`tools.jackson`, managed at 3.1.7) while `config.yaml` and `attribution_json` stay on Jackson 2
  (`com.fasterxml`, 2.21.7). Both lines are on purpose; one BOM is what keeps each internally
  consistent.
- The BOM's JUnit pin wins even when the pom asks for another version, because it manages
  `junit-jupiter-api`, `-engine` and `-params` individually. Declaring `junit-jupiter:5.11.4` still
  resolved those three to 6.0.3. The version properties are therefore gone, and the whole test stack
  is JUnit 6.0.3 with AssertJ 3.27.7 -- the whole suite resolving under surefire 3.5.2 is the evidence.

`pom.xml` carries the same notes where they are acted on, and it is the file to read before
rearranging anything there.

## Flink 2.x Breaking Changes That Affect This Project

These are the traps if you copy code from tutorials (most of which target Flink 1.x):

| Removed in 2.0 | Use instead | Where it bites us |
|----------------|-------------|-------------------|
| `SinkFunction`, `Sink V1` | **Sink V2** (`Sink`, `SinkWriter`, `InitContextProvider`) | Our `pipeline/sink/DuckDbSink` -- the existing PyFlink custom sink has no 1:1 translation |
| `SourceFunction` | **FLIP-27 `Source`** | Kafka reading; `KafkaSource` already complies |
| `DataSet` API | `DataStream` / Table API | Not used |
| `flink-conf.yaml` | `config.yaml` (standard YAML) | Cluster config file name |
| `Time` class | `java.time.Duration` | `TumblingProcessingTimeWindows.of(Duration.ofSeconds(60))` |

Also: JDK 17 enforces modularization, so reflective access needs
`--add-opens java.base/java.util=ALL-UNNAMED`. This is already configured in
`pom.xml` (surefire + exec) and `.vscode/launch.json`. If you run from a plain terminal,
add it yourself.

## Package Layout

```
backend/src/main/
|-- resources/
|   |-- config.yaml           # loaded from the classpath, no cwd-relative config path
|   |-- application.properties # Spring's own settings only; currently the port
|   |-- schema.sql            # CREATE TABLE IF NOT EXISTS, all seven tables
|   `-- logback.xml           # Flink and pekko at WARN; without it Logback defaults to root DEBUG
`-- java/com/fyp/
    |-- AttributionJob.java           # main(): run the pipeline, print the results
    |-- ApiApplication.java           # main(): the HTTP surface, three beans and nothing else
    |-- RunContext.java               # runId, startedAt, dataset, config snapshot
    |-- Main.java                     # scratch sample from the JDK setup, not part of the pipeline
    |-- api/                # RunController (POST /runs, GET /runs, GET /runs/{runId}) +
    |                     # DatasetController (POST /datasets and its three reads) -- the only
    |                     # classes that mention Spring
    |-- service/            # AttributionService (run, fetchAll, findRun, listRuns) + DatasetService
    |                     # (store, list, find, delete, collectExpired): what a controller is allowed
    |                     # to talk to
    |-- config/             # AppConfig + DatasetConfig/FlinkConfig/PipelineConfig records, ConfigLoader
    |-- model/              # Tweet, UnifiedEvent, Attribution, WindowOutput
    |-- pipeline/
    |   |-- loader/         # CsvDatasetLoader -> List<Tweet>, DatasetScan (upload check),
    |   |                   # DatasetFormatException
    |   |-- flink/          # HashtagExtractor, EventTimestampAssigner, CategoryAttributionFunction
    |   `-- sink/           # DuckDbSink + DuckDbSinkWriter (Sink V2, batch 100)
    `-- storage/            # SqlExecutor (interface), DuckDbExecutor, SchemaInitializer,
                            # RunRepository, WindowOutputRepository, DatasetRepository + Dataset,
                            # Row, RunResults, Run
```

`api/` is the only package that may mention Spring, and `ApiApplication` is the only file that
annotates anything. Everything under `config/`, `model/`, `pipeline/`, `service/` and `storage/` stays
container-free so the pipeline runs from `AttributionJob.main` and from its tests with no context
loaded. The split is one deep: a controller calls a service, a service calls repositories, and nothing
calls upward.

SQL lives in `storage/`, Flink lives in `pipeline/`, and the sink reaches the database only through
the `SqlExecutor` interface, which is why `DuckDbSinkWriterTest` can count batches with no database
attached (`RecordingExecutor`).

Two deliberate deviations from `legacy/docs/design/reference_schema.md`, both in `window_outputs`,
documented in `schema.sql`:

1. `output_id` draws from a sequence. A plain `INTEGER PRIMARY KEY` is not filled in by DuckDB when
   the column is omitted, and `SERIAL` and `GENERATED AS IDENTITY` are both rejected by the 2.0
   driver, so the sequence is named explicitly. Without this the second run collides on key `1`.
2. `category` holds the hashtag each window was keyed by. The reference document predates hashtag
   keying, so two hashtags in one window would otherwise be indistinguishable.

## Running

```cmd
:: the pipeline: reads dataset.target_dataset, writes ../data/attribution.duckdb
run.bat

:: compile only
run.bat compile

:: the HTTP API on :8081 (application.properties), Ctrl-C to stop
run.bat serve

:: the tests
run.bat test

:: delete backend\target
run.bat clean

:: move the dev database and every uploaded csv out of data\ (stop serve first)
run.bat reset
```

`run.bat reset` is the development wipe: `attribution.duckdb` (so every run, every window output and
every `datasets` row goes with it) and `uploads\ds-*.csv`, all moved to `..\data\cleared-<when>\`
rather than deleted. Nothing in the API can do this -- there is no `DELETE` for runs, and
`DELETE /datasets/{id}` forgets one upload at a time. The next `run.bat serve` applies `schema.sql` --
seven tables plus `window_outputs_output_id_seq` -- while it builds the pipeline service, before it
accepts a request, so after a reset `GET /runs` and `GET /datasets` both answer `[]`. It refuses to
touch anything while something is listening on the port in `application.properties`, because that server
holds the only open handle on the file and clearing uploads underneath it would leave it serving ids
whose csvs are gone. That check is only as good as the port it reads: edit `server.port` while a server
is still up on the old one and the guard looks at a port nothing listens on, then wipes underneath the
running process -- which is how a `del`eted-but-open `attribution.duckdb` gets archived as a `.wal` with
no main file next to it.

`run.bat` takes exactly one word and dispatches on it; it does not forward extra arguments. For the
Maven goals underneath, call Maven yourself from `backend/`. The pom reads the `mainClass` property,
not `exec.mainClass`:

```cmd
mvn compile exec:exec "-DmainClass=com.fyp.ApiApplication"
```

If `mvn` is not on `PATH`, use the copy `run.bat` prints at the top of every run
(`%USERPROFILE%\.m2\wrapper\dists\apache-maven-3.9.9\<hash>\bin\mvn.cmd`).

Expected output of a run, after the Flink shutdown noise:

```
run-20261114-1516-3fa4: 3 window outputs, 57 hashtag events counted from tweet_hashtag.csv
[1793844000000,1793844060000) AI         count=21   {"user_1":4,"user_2":10,"user_3":7}
[1793844000000,1793844060000) DATA       count=18   {"user_1":3,"user_2":8,"user_3":7}
[1793844000000,1793844060000) UST        count=18   {"user_1":5,"user_2":5,"user_3":8}
```

Use `exec:exec`, **not** `exec:java`. `exec:java` runs inside the Maven JVM and cannot pass the
`--add-opens` flags Flink needs under JDK 17; the pom only configures `exec:exec`, so `exec:java`
fails with a parameter-marshalling error.

## The Packaged Jar

`mvn package` now produces a Spring Boot executable jar (`spring-boot-maven-plugin` replaced
`maven-shade-plugin`), which changes two things that used to hold:

- **`flink run -c com.fyp.AttributionJob target\...jar` no longer works.** Boot's layout puts the
  classes in `BOOT-INF/classes` behind a launcher, which is not what a cluster's job submission
  expects. There is no cluster, so nothing was broken in practice; restoring it means either a
  second Maven module holding the job alone, or putting shade back for a pipeline-only artifact.
- **`java -jar` starts the API but cannot run the job.** Boot keeps dependencies as nested jars,
  reachable only through its own classloader, and Flink deserialises the stream graph with a plain
  `Class.forName`. So `POST /runs` answers 500 with
  `ClassNotFoundException: org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory`
  even though the pipeline runs fine through `exec:exec`, which uses a flat classpath.

Boot's own `tools` jar mode is the fix, and it is one command:

```cmd
mvn -q package
java -Djarmode=tools -jar target\fyp-attribution-backend-0.1.0.jar extract --destination target\extract
java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED ^
     -jar target\extract\fyp-attribution-backend-0.1.0.jar
```

Extraction writes a thin jar whose manifest `Class-Path` lists `lib\*.jar`, so those land on the
**system** classpath and `Class.forName` finds them. Verified: the third command serves `POST /runs`
and stores real results. This is also the shape you would deploy, so prefer it over running the
nested jar directly.

## HTTP API

Field-by-field reference, including every error message a client can receive:
**[`docs/API.md`](docs/API.md)**. This section is the reasoning; that one is what to code against.
[`../frontend/backend_api_reference.md`](../frontend/backend_api_reference.md) is the client-side
summary, and [`../frontend/backend_test_script.py`](../frontend/backend_test_script.py) drives all
seven endpoints against a live server (`python frontend/backend_test_script.py`, requests only).

Two resources so far, each in a controller named after the noun in its path rather than the storage
behind it: `api/RunController.java` for runs and `api/DatasetController.java` for uploads. A run is
`POST`, not `PUT`, because the same dataset run twice is two runs with two ids rather than one resource
written twice. The port is `server.port` in `src/main/resources/application.properties` -- 8081,
because Kafka UI publishes 8080 in `docker/docker-compose.yml`.

Neither controller holds a repository. Each is handed services and nothing else -- `RunController`
takes `AttributionService` and `DatasetService`, `DatasetController` takes `DatasetService` -- so the
HTTP layer never learns SQL, file names or which lock guards what. Start the server once with
`run.bat serve`; the curls below all assume it is up.

### `POST /runs` -- run a stored dataset

```cmd
curl -X POST -H "Content-Type: application/json" -d "{\"datasetId\":\"ds-20261116-0646-236e\"}" http://localhost:8081/runs
```

```json
{"runId":"run-20261116-0646-236e"}
```

| Response | When |
|----------|------|
| `200 {"runId": "..."}` | every window of that dataset is attributed and stored; `GET /runs/{runId}` reads them back |
| `400 {"error": "..."}` | the body had no `datasetId`, or the stored file is not a valid tweets CSV any more -- the message says which line or header failed |
| `404 {"error": "no dataset with id ..."}` | nothing was uploaded under that id, or its file has gone. No run row is written. |
| `500 {"error": "..."}` | the data was fine and our own storage failed |
| `415`, Spring's own body | a content type that is not `application/json` -- which is what a client still posting csv bytes gets |

Four things worth knowing before extending it:

- **The body names an upload, it is not one.** `POST /datasets` scanned the file before saving it, and
  that scan is what proves the timestamps are sorted -- the job assigns watermarks monotonically, so
  unsorted input answers 200 and silently drops every event that looks late. Refusing bytes here makes
  that input impossible to run.
- The run is **synchronous**. Two seconds at 50 rows, and 200 means the results are already stored.
  At larger inputs the same run id supports `202 Accepted` plus a status endpoint -- `GET /runs/{runId}`
  already reads back `started_at` and `finished_at`, so the change is the POST returning early.
- Runs are **serialised on one lock**. `AttributionService` holds a single DuckDB connection and
  DuckDB allows one writer per file, so two overlapping requests would interleave their batches.
  Queueing properly is the fix if this ever serves more than one user.
- **`runs.dataset` holds the id you sent**, so the run row points back at the file it read and
  `GET /runs?datasetId=` is a where clause rather than a new feature. The command line job has no id to
  name, so it writes `dataset.target_dataset` from `config.yaml` into that column instead.

### `GET /runs` and `GET /runs/{runId}` -- read back what ran

```cmd
curl http://localhost:8081/runs
curl http://localhost:8081/runs/run-20261116-0646-236e
```

The list is every run row, newest first, as `{"runId", "startedAt", "finishedAt", "dataset"}` -- enough
to draw a run picker in one call. The detail is that same row plus `"windows"`, one entry per firing
window with its `category`, `outputCount` and the parsed `attribution`, ordered by `window_start` then
`category`. That is the whole answer set, so the frontend no longer has to open the DuckDB file to
render a run.

Three decisions in there worth keeping if you extend it:

- **`200` with `"windows": []` is not a 404.** The run row is written before the csv is read, so a run
  that failed has an id and no results. An id that never ran is the only 404, and `finishedAt` is what
  separates an empty run that is still going from one that broke.
- **The reads take `runLock` too.** They only SELECT, but they share the one connection with a job that
  is INSERTing, so a `GET` that lands mid-run waits for it instead of interleaving. Moving the lock
  into `AttributionService` -- and giving reads their own connection -- is task B3.
- **`windowStart` and `windowEnd` stay epoch milliseconds.** They are the same unit as the csv
  timestamps and the same values the column holds; converting them to ISO strings here would mean the
  dashboard converts back.
- **The tables are there before the first request.** `ApiApplication.attributionService` calls
  `AttributionService.ensureSchema()` while building the bean, so `schema.sql` is applied when the
  server starts rather than when something is first written. Every statement is
  `CREATE TABLE IF NOT EXISTS`, so on a database that has been used before the call costs milliseconds
  and changes nothing. It is there because an empty database is a state a client has a right to see:
  with the schema only in the write paths, `GET /runs` straight after `run.bat reset` answered 500 with
  `Catalog Error: Table with name runs does not exist!` instead of `[]`.

### `/datasets` -- keep an upload

`POST /datasets` is what makes an upload runnable by id. `DatasetScan` checks the csv first, then the
bytes are written verbatim to `dataset.uploads_dir` under a fresh id and a `datasets` row records what
was stored. Nothing runs, so the call is fast and returns before any window is computed.

```cmd
curl -X POST -H "Content-Type: text/csv" --data-binary @..\data\raw\tweet_hashtag.csv "http://localhost:8081/datasets?name=october.csv"
```

```json
{"datasetId":"ds-20261116-0646-236e","originalName":"october.csv","uploadedAt":"2026-11-16T06:46:00Z","expiresAt":null,"sizeBytes":1957,"rowCount":50}
```

| Endpoint | Response | When |
|----------|----------|------|
| `POST /datasets` | `201 {"datasetId": ...}` with `Location: /datasets/{id}` | stored, and `rowCount` is the number of tweets the loader will see |
| | `400 {"error": "..."}` | the csv failed the scan; the message names the header or the line that is out of order |
| | `415` / `400` | body sent as a form / body empty |
| `GET /datasets` | `200 [...]` | every upload, newest first |
| `GET /datasets/{id}` | `200` / `404` | one row, or `no dataset with id ...` |
| `DELETE /datasets/{id}` | `204` / `404` | row and file both gone, or nothing matched |

Three things about the shape of that response:

- **`name` rides in the query** because a raw-bytes body cannot carry a filename. It is only a label:
  directories are stripped, so `../../desktop/tweets.csv` is stored as `tweets.csv`, and the file on
  disk is always named from the id, never from the client.
- **`rowCount` is measured, not trusted.** The scan checks the header and that timestamps never go
  backwards, because the job assigns watermarks monotonically and an unsorted upload silently loses
  rows rather than failing. Checking at upload time says so in the request that caused it.
- **`expiresAt` is null while `dataset.upload_ttl_days` is zero**, which is the shipped default: keep
  every upload until it is deleted by hand. Set it to a positive number and the field becomes a date.
  `DatasetService.collectExpired()` deletes what has passed, and nothing schedules that call yet -- a
  cron entry or a Spring `@Scheduled` method can both drive it unchanged.

## Also Worth Knowing

- Only **one process at a time** may hold `../data/attribution.duckdb`. Start `run.bat serve` twice,
  or run the CLI job while the API is up, and the second one dies in `DuckDbExecutor` with a
  "File is already open in ... (PID n)" message that names the offender. This is DuckDB's single
  writer, not a bug, and it is also why `RunController` locks runs inside one process.
- The jar currently **bundles Flink core** (`flink-streaming-java`, `flink-clients` are in
  `compile` scope). A cluster that already provides Flink will hit classloader conflicts. The fix
  is `provided` scope, but that also removes them from `exec:exec`'s runtime classpath, so local
  MiniCluster runs need `-Dexec.classpathScope=test` afterwards. Leave as-is until a cluster exists.
- DuckDB and dataset paths in `config.yaml` resolve **relative to `backend/`**, because that is the
  working directory every run route uses. `java -jar` from the repo root looks for `../data/`
  outside the project and fails with a Chinese-locale "path not found" message; `cd backend` first.
- `logback.xml` pins `org.apache.flink` and `org.apache.pekko` to WARN. Without it Logback falls
  back to its built-in root **DEBUG**, and because Flink core is compile-scoped a single run printed
  524 console lines, 325 of them `MetricRegistryImpl` registering gauges. It is now 27. Flip those
  two loggers back to DEBUG to watch watermark advancement when a window fires no output. Three
  further classes are muted to ERROR in `logback.xml`; each one prints a stack trace that is only
  local-mode-on-Windows noise, and the file says which.

## Tests

```cmd
run.bat test
mvn -Dtest=AttributionPipelineEndToEndTest test
```

108 tests: pure ones for config, hashtag extraction, CSV loading and attribution arithmetic; MiniCluster
ones for the window firing; `RecordingExecutor` ones for batch shape and SQL, including the two run
reads `GET /runs` and `GET /runs/{runId}` sit on; and
`AttributionPipelineEndToEndTest`, which runs CSV to a real DuckDB file in a temp directory twice to
prove two runs coexist. `flink-test-utils` gives the MiniCluster, so no Kafka and no cluster needed.

`DatasetScanTest` and `DatasetRepositoryTest` follow the same split: the scanner is a function from
bytes to a row count, so it needs no database, and the repository is tested against a recording
`SqlExecutor` and a `@TempDir`, so the SQL it emits and the files it writes are both asserted without
DuckDB or a clock in the way. `DatasetServiceTest` then checks the only thing the service adds -- that
it hands the repository an `Instant` from now.

The API classes split by how much they drag in. `RunControllerTest` and `DatasetControllerTest` stub
their service and use `standaloneSetup`, so they check status codes and body shape in about a second.
`ApiRoundTripTest` boots the real context on a random port and drives both `/runs` and `/datasets` over
HTTP into a temp DuckDB and a temp uploads directory -- that is the one that notices a broken bean or a
converter that cannot serialise a record, and it is what proves the upload -> run-by-id -> read-the-run
back chain works end to end. `EmptyDatabaseReadTest` boots a second context over a database file that
nothing has written to yet and expects both `GET`s to answer `[]`, which is the only test that can see
the startup schema: every test in `ApiRoundTripTest` writes before it reads, so the tables are already
there by the time it asks. None of them loads a context for the pipeline tests, which is why the two
services are injected into the controllers rather than annotated anywhere below `api/`.
