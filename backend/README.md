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
| Provenance graph: `tin_vertices` + `tin_edges` per run, and a `tin_states` row for what each box holds | working. `K1`'s and `M1`'s boxes and every `W1` firing; `runs.provenance_enabled` is written as `true` |
| `tin_interactions` -- what an edge carried, counted on that edge | working: `K1 -> M1` in tweets (`category` is `null` there), `M1 -> W1` in hashtag events, `W1 -> S1` the results the sink was handed -- one row per (edge, interval[, category]) |
| The three intervals (`window_size_seconds`, `state_tick_seconds`, `interaction_tick_seconds`) | working, all three shipped at 60. A tick that does not divide the window is refused when the config is read. `W1` cannot be snapshotted mid-window yet -- see `docs/TIN.md` |
| `GET /runs/{runId}/graph` -- the four `tin_*` tables as one `{vertices, edges, states, interactions}` body | working, see `docs/API.md`. `run.bat graph <runId>` prints the same four tables straight out of the database and `frontend/test.py` now prints this route's body, so both reads are exercised; neither is a display -- the Streamlit dashboard is still not started |
| The four invariants of `docs/TIN.md` checked against stored rows | working: `ProvenanceInvariantTest` runs 1 to 3 over a fixture whose ticks are finer than its window, and `AttributionPipelineEndToEndTest` reads `K1`'s box against the csv row count for 4 |
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
files under JUnit temp directories, so all 138 tests passing means the JDK, Flink and the DuckDB driver
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
|   |-- schema.sql            # CREATE TABLE IF NOT EXISTS, all seven tables and three id sequences
|   `-- logback.xml           # Flink and pekko at WARN; without it Logback defaults to root DEBUG
`-- java/com/fyp/
    |-- AttributionJob.java           # main(): run the pipeline, print the results
    |-- ApiApplication.java           # main(): the HTTP surface, three beans and nothing else
    |-- GraphReport.java              # main(): run.bat graph, one stored run's four tin_* tables
    |-- RunContext.java               # runId, startedAt, dataset, config snapshot
    |-- Main.java                     # scratch sample from the JDK setup, not part of the pipeline
    |-- api/                # RunController (POST /runs, GET /runs, GET /runs/{runId} and its
    |                     # /graph) + DatasetController (POST /datasets and its three reads) -- the
    |                     # only classes that mention Spring
    |-- service/            # AttributionService (run, fetchAll, findRun, listRuns, findGraph) +
    |                     # DatasetService (store, list, find, delete, collectExpired): what a
    |                     # controller is allowed to talk to
    |-- config/             # AppConfig + DatasetConfig/FlinkConfig/PipelineConfig records, ConfigLoader
    |-- model/              # Tweet, UnifiedEvent, Attribution, WindowOutput, ProvenanceRecord,
    |                     # Vertex, Edge, State, Interaction, RunVertices, RunGraph
    |-- pipeline/
    |   |-- loader/         # CsvDatasetLoader -> List<Tweet>, DatasetScan (upload check),
    |   |                   # DatasetFormatException
    |   |-- flink/          # HashtagExtractor, TweetTimestampAssigner, CategoryAttributionFunction,
    |   |                   # BoxRecorder (a vertex's box per interval), InteractionRecorder (an edge's
    |   |                   # traffic per interval)
    |   |-- graph/          # ProvenanceGraph: mints K1/M1/F1/W1/S1 for one run and links them
    |   `-- sink/           # DuckDbSink + DuckDbSinkWriter (Sink V2, batch 100)
    `-- storage/            # SqlExecutor (interface), DuckDbExecutor, SchemaInitializer,
                            # RunRepository, WindowOutputRepository, ProvenanceRepository,
                            # DatasetRepository + Dataset, Row, RunResults, Run
```

`api/` is the only package that may mention Spring, and `ApiApplication` is the only file that
annotates anything. Everything under `config/`, `model/`, `pipeline/`, `service/` and `storage/` stays
container-free so the pipeline runs from `AttributionJob.main` and from its tests with no context
loaded. The split is one deep: a controller calls a service, a service calls repositories, and nothing
calls upward.

SQL lives in `storage/`, Flink lives in `pipeline/`, and the sink reaches the database only through
the `SqlExecutor` interface, which is why `DuckDbSinkWriterTest` can count batches with no database
attached (`RecordingExecutor`).

## Provenance Capture

A run describes itself in all four `tin_*` tables. `AttributionService` mints one vertex per operator it
adds -- `K1` the csv source, `M1` the hashtag extractor, `W1` the window, `S1` the sink -- and connects
them in the same order, so the topology is recorded where it is built rather than reconstructed from the
results afterwards. The four ids then travel to the sink inside one `RunVertices` record, and one flush
still writes all three result tables, so a firing's `window_outputs` row, its `tin_states` row and its
`W1 -> S1` interaction cannot disagree. Ids come only
from `ProvenanceGraph` -- nothing in `pipeline/`, `service/` or `storage/` spells one out -- so a second
map operator is `M2` and a second chain is `K2` with nothing else to edit. (`model/TINs.java` is the
standalone prototype of the reference schema and names its own vertices in memory; the pipeline does not
read it.)

Each number is now taken where the data carrying it is. `BoxRecorder` sits on the stream into an operator
and writes two things per interval: the box -- `K1`'s rows read so far, `M1`'s events emitted so far, both
cumulative -- and the interaction out of it. `InteractionRecorder` sits on the keyed stream into the window
and writes one row per hashtag per interval. The firing writes only its own `W1 -> S1` row, which is the
honest claim: the sink is what the window handed it. So `K1 -> M1` counts *tweets*, hashtag-free included,
while `M1 -> W1` counts *events* -- the two differ whenever one tweet names more than one tag, and that gap
is something a client can now read instead of infer. All three row kinds are stamped with the end of the
interval they describe.

What the sink is handed is `ProvenanceRecord`: three kinds -- a fired window, one vertex's state over one
interval, one interaction over one edge -- over one stream, because one writer opens DuckDB (the file
allows one read-write process, and the retry protection is one flush plus a natural unique key).
`AttributionService` builds four branches over the same two streams and `union(...)`s them into that sink,
which is what keeps `BoxRecorder` from needing a connection of its own. Event time is stamped on the source
stream, before the extractor, so a stage upstream of the window can be bucketed by interval at all.

Five deliberate deviations from the TIN reference schema, each one a place the document predates this
job, listed in `schema.sql`:

1. Every surrogate id draws from a named sequence. A plain `INTEGER PRIMARY KEY` is not filled in by
   DuckDB when the column is omitted, and `SERIAL` and `GENERATED AS IDENTITY` are both rejected by the
   2.0 driver -- so `window_outputs_output_id_seq`, `tin_states_state_id_seq` and
   `tin_interactions_interaction_id_seq` are declared and referenced explicitly. Without this the
   second run collides on key `1`.
2. `category` holds the hashtag each row was keyed by. The reference document predates hashtag keying,
   so two hashtags in one window would otherwise be indistinguishable -- on `window_outputs`,
   `tin_states` and `tin_interactions` alike, since `W1` is one vertex that fires one state and three
   interactions per (window, category).
3. `tin_vertices` is keyed by `(run_id, vertex_id)`, not `vertex_id`. Ids are minted per run by counting
   operator types, so every run calls its window operator `W1` and a global key would reject the second.
4. Natural `UNIQUE` keys on the tables a run writes. The JDBC sink is at-least-once, so a retried batch
   has to collide with something rather than land as an extra row.
5. `category` is nullable on `tin_states` and `tin_interactions`, because a `K1` or `M1` row is about
   tweets rather than one hashtag. `window_outputs` keeps `NOT NULL` -- every row there is one hashtag's
   result. The measured cost: DuckDB treats two null categories as distinct, so the key in 4 does not
   collide for those rows and `UNIQUE NULLS NOT DISTINCT` does not parse on this driver.
   `ProvenanceRepositoryTest` asserts the hole instead of hiding it.

## Running

```cmd
:: the pipeline: reads dataset.target_dataset, writes ../data/attribution.duckdb
run.bat

:: compile only
run.bat compile

:: the HTTP API on :8000 (application.properties), Ctrl-C to stop
run.bat serve

:: print the four tin_* tables of one stored run: reads only, starts no job
run.bat graph run-20261122-1725-2282

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
seven tables plus the three id sequences -- while it builds the pipeline service, before it
accepts a request, so after a reset `GET /runs` and `GET /datasets` both answer `[]`. It refuses to
touch anything while something is listening on the port in `application.properties`, because that server
holds the only open handle on the file and clearing uploads underneath it would leave it serving ids
whose csvs are gone. That check is only as good as the port it reads: edit `server.port` while a server
is still up on the old one and the guard looks at a port nothing listens on, then wipes underneath the
running process -- which is how a `del`eted-but-open `attribution.duckdb` gets archived as a `.wal` with
no main file next to it.

Reset it after editing `schema.sql`, not just when you want a clean slate. Every statement is
`CREATE TABLE IF NOT EXISTS`, which means an existing table keeps whatever columns it was first created
with: a database that predates the `tin_*` fixes still holds the old definitions, and the new inserts
fail on the missing columns until the file goes.

`run.bat` dispatches on its first word. Only `graph` takes a second one, and even that is not a program
argument: Maven's `exec:exec` builds its `java` command line from the `<arguments>` list in `pom.xml` and
forwards nothing from the caller, so `graph` passes the id as `-DrunId=...`, the pom has an empty `runId`
property so the other routes' command lines are unchanged, and `GraphReport` reads the property. For the
Maven goals underneath, call Maven yourself from `backend/`. The pom reads the `mainClass` property,
not `exec.mainClass`:

```cmd
mvn compile exec:exec "-DmainClass=com.fyp.ApiApplication"
mvn compile exec:exec "-DmainClass=com.fyp.GraphReport" "-DrunId=run-20261122-1725-2282"
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

`run.bat graph <runId>` is the read-only half: no job, no rows written, and the same four tables
`GET /runs/{runId}/graph` returns. It is the route that shows what the capture actually stored, because
`run.bat` itself prints only `window_outputs`. This is real output from `..\data\attribution.duckdb`:

```
run-20261122-1725-2282: 4 vertices, 3 edges, 5 states, 7 interactions

== tin_vertices ==
K1   | source   | csv dataset tweet_hashtag (50 rows)
M1   | map      | hashtag extractor, one event per hashtag
S1   | sink     | duckdb sink, window_outputs, tin_interactions and tin_states
W1   | window   | 60s tumbling count, keyed by category

== tin_edges ==
K1 -> M1
M1 -> W1
W1 -> S1

== tin_states == what the box held; the K1 and M1 counts are cumulative
K1   | -        | [1793844000000, 1793844060000) | qty=50    | fired_at=1793844060000  | {}
M1   | -        | [1793844000000, 1793844060000) | qty=57    | fired_at=1793844060000  | {}
W1   | AI       | [1793844000000, 1793844060000) | qty=21    | fired_at=1793844060000  | {"user_1":6,"user_2":8,"user_3":7}
W1   | DATA     | [1793844000000, 1793844060000) | qty=29    | fired_at=1793844060000  | {"user_1":20,"user_2":3,"user_3":6}
W1   | UST      | [1793844000000, 1793844060000) | qty=7     | fired_at=1793844060000  | {"user_1":4,"user_3":3}

== tin_interactions == what the edge carried; keys name vertices, never users
K1 -> M1   | -        | ts=1793844060000  | qty=50    | {"K1":50}
M1 -> W1   | AI       | ts=1793844060000  | qty=21    | {"M1":21}
M1 -> W1   | DATA     | ts=1793844060000  | qty=29    | {"M1":29}
M1 -> W1   | UST      | ts=1793844060000  | qty=7     | {"M1":7}
W1 -> S1   | AI       | ts=1793844060000  | qty=21    | {"W1":21}
W1 -> S1   | DATA     | ts=1793844060000  | qty=29    | {"W1":29}
W1 -> S1   | UST      | ts=1793844060000  | qty=7     | {"W1":7}
```

Read the two unit halves against each other: `K1` held 50 tweets and passed 50 over `K1 -> M1`, while
`M1` held 57 events because one tweet naming two hashtags is two events -- the 21 + 29 + 7 of the three
`M1 -> W1` rows. A `-` in the category column is a `null`, which means that row's operator was never keyed
by hashtag; it is not missing data. An id that never ran says so and lists the ids this database does hold.

It refuses while a server is listening on the port in `application.properties`, because DuckDB allows one
process per file -- read those rows over HTTP instead while `run.bat serve` is up.

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
The only client in the repository is [`../frontend/test.py`](../frontend/test.py) -- one function per
endpoint, `requests` only. Its `main()` is the whole path in order: upload the five-tweet fixture, run
it, then print `GET /runs/{runId}` and `GET /runs/{runId}/graph` over the run id it just got, so one
`python frontend/test.py` against a live server exercises every endpoint the provenance capture feeds.
To check a different doc claim, edit that `main()`. Its `BASE_URL` is a constant at the top of the file,
not read from the backend, so it has to be edited whenever the port below changes.

Two resources so far, each in a controller named after the noun in its path rather than the storage
behind it: `api/RunController.java` for runs and `api/DatasetController.java` for uploads. A run is
`POST`, not `PUT`, because the same dataset run twice is two runs with two ids rather than one resource
written twice. The port is `server.port` in `src/main/resources/application.properties` -- 8000, which
clears both maps in `docker/docker-compose.yml` (Kafka `9092`, Kafka UI `8080`). `run.bat serve` cannot
override it on the command line: the exec plugin's argument list in `pom.xml` is fixed, so a trailing
`--server.port=...` is silently dropped. Edit the file, or set `SERVER_PORT` in the environment.

Neither controller holds a repository. Each is handed services and nothing else -- `RunController`
takes `AttributionService` and `DatasetService`, `DatasetController` takes `DatasetService` -- so the
HTTP layer never learns SQL, file names or which lock guards what. Start the server once with
`run.bat serve`; the curls below all assume it is up.

### `POST /runs` -- run a stored dataset

```cmd
curl -X POST -H "Content-Type: application/json" -d "{\"datasetId\":\"ds-20261116-0646-236e\"}" http://localhost:8000/runs
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
curl http://localhost:8000/runs
curl http://localhost:8000/runs/run-20261116-0646-236e
```

The list is every run row, newest first, as `{"runId", "startedAt", "finishedAt", "dataset"}` -- enough
to draw a run picker in one call. The detail is that same row plus `"windows"`, one entry per firing
window with its `category`, `outputCount` and the parsed `attribution`, ordered by `window_start` then
`category`. That is the whole answer set, so the frontend no longer has to open the DuckDB file to
render a run.

Four decisions in there worth keeping if you extend it:

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

### `GET /runs/{runId}/graph` -- read back the machinery

```cmd
curl http://localhost:8000/runs/run-20261116-0646-236e/graph
```

One SELECT per `tin_*` table and the rows handed back as `{vertices, edges, states, interactions}`,
which is `G = (V, E, R)` plus `S` in the shape a graph library takes without a database driver in the
client. `service.findGraph` builds a `ProvenanceRepository` over the same `SqlExecutor` the write paths
use, and the controller maps the four model records to four response records -- the `runId` field the
model types carry is dropped, because the caller just put it in the path.

Four decisions here, all of them there to keep the read honest rather than convenient:

- **It is its own route, not extra keys on `GET /runs/{runId}`.** That endpoint's `windows` array is
  what the dashboard draws and is one row per fired window; this body is four arrays of a different
  shape that a caller reads only when it wants the provenance. Merging them would make every results
  fetch pay for four queries it does not ask for.
- **The 404 comes from `runs`, not from `tin_vertices`.** An id with no topology rows but a run row is a
  run that broke before the topology write, and it answers `200` with empty arrays. The existence check
  is the same one `GET /runs/{runId}` uses, so the two cannot disagree about whether a run happened.
- **`vertices` comes back sorted by id, and ids are strings** -- so `S1` precedes `W1`. Ordered by
  `vertex_id` because that is what makes two runs' rows comparable in a test; a caller that wants
  pipeline order walks `edges` from the `source` vertex.
- **A state row means what its vertex holds, which is not one thing yet.** `W1`'s rows are fired windows,
  so `states` filtered to `W1` still duplicates `windows` -- deliberately, since it is the same measurement
  written by the same flush. `K1`'s and `M1`'s rows are running boxes, cumulative over the run, with an
  empty `attribution` because neither operator ever held a per-user tally. That asymmetry is the reason the
  endpoint returns four arrays rather than one joined table: a `quantity` only means something next to the
  `vertexId` that measured it.

### `/datasets` -- keep an upload

`POST /datasets` is what makes an upload runnable by id. `DatasetScan` checks the csv first, then the
bytes are written verbatim to `dataset.uploads_dir` under a fresh id and a `datasets` row records what
was stored. Nothing runs, so the call is fast and returns before any window is computed.

```cmd
curl -X POST -H "Content-Type: text/csv" --data-binary @..\data\raw\tweet_hashtag.csv "http://localhost:8000/datasets?name=october.csv"
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

138 tests: pure ones for config, hashtag extraction, CSV loading and attribution arithmetic; MiniCluster
ones for the window firing and for the two recorders -- `BoxRecorderTest` is the one that notices if a box
ever stops accumulating across intervals, `InteractionRecorderTest` the one that notices if a hashtag which
said nothing in a tick starts writing a row about it; `RecordingExecutor` ones for batch shape and SQL,
including the run reads `GET /runs`, `GET /runs/{runId}` and `GET /runs/{runId}/graph` sit on;
real-DuckDB ones for the repositories whose tables have
sequences and unique keys (`ProvenanceRepositoryTest` checks that `state_id` and `interaction_id` fill
themselves, that the interactions of one chain read back in edge order, that a repeated state or interaction
is rejected when the row carries a hashtag, and that it is *not* rejected when the column is null --
DuckDB treats two nulls as distinct, so the retry guard covers the hashtag-bearing rows only); and
`AttributionPipelineEndToEndTest`, which runs CSV to a real DuckDB file in a temp directory twice to
prove two runs coexist -- including their own `K1`/`M1`/`W1`/`S1`, their own five states and their own
seven interactions; and `ProvenanceInvariantTest`, which runs invariants 1 to 3 of `docs/TIN.md` over those
stored rows -- a box's growth against the edge carrying its unit, a window's count against the sum of the
ticks that fed it, a firing's state against what the sink was handed. Its fixture is the only one whose
ticks are finer than its window, because with a single interval all three checks compare one number with
itself.
`flink-test-utils` gives the MiniCluster, so no Kafka and no cluster needed.

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
back chain works end to end. Its success test reads the graph of the run it just made and counts the
rows the job actually wrote -- four vertices, three edges, and five rows across `states` and
`interactions` per window -- plus the two facts a client reads them for: the `K1` and `M1` rows arrive
with `category: null` and an empty `attribution`, and the `K1 -> M1` interaction is one row of tweets rather
than one per hashtag. So a sink that stopped reporting provenance, or a recorder that drifted back to
user keys, fails there rather than only in a repository test.
`EmptyDatabaseReadTest` boots a second context over a database file that
nothing has written to yet and expects both `GET`s to answer `[]`, plus `404` for
`GET /runs/{runId}/graph` of an id that never ran, which is the only test that can see
the startup schema: every test in `ApiRoundTripTest` writes before it reads, so the tables are already
there by the time it asks. None of them loads a context for the pipeline tests, which is why the two
services are injected into the controllers rather than annotated anywhere below `api/`.
