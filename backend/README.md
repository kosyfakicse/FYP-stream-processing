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
**DuckDB is the only contract between backend and frontend** -- no REST server, no message bus.

## What Is Implemented

| Path | Status |
|------|--------|
| CSV dataset -> hashtags -> event-time tumbling windows -> per-source attribution -> DuckDB | working, see `AttributionJob` |
| `runs` + `window_outputs` writes and run-scoped retrieval | working |
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

## First-Time Setup

Only the JDK needs a system install. `run.bat` injects `JAVA_HOME` for that one command so it never
touches your global environment. This matters here: a pre-existing JDK 20 on `PATH` otherwise
shadows the Temurin 17 build.

Run every command from inside `backend/`.

```cmd
:: 1. Install JDK 17 only (elevated terminal)
winget install EclipseAdoptium.Temurin.17.JDK

:: 2. Verify the toolchain (MiniCluster in-process, no cluster or Kafka needed)
run.bat smoke

:: 3. Run the pipeline on the configured dataset, then read the results back by run id
run.bat

:: 4. Run the tests
run.bat test
```

Step 2 is the gate. Expected output, with the Flink line being the project's known-answer result:

```
Java release = 17
Flink output: 3 #python in window [1000,2000) {user_1=1, user_2=1, user_4=1}
DuckDB engine version = v2.0.0
DuckDB row count = 2
All toolchain checks passed.
```

`run.bat` compiles and runs through Maven rather than a hand-written `javac` line on purpose: the
compile classpath is 108 jars (278 MB), longer than the 1023 characters cmd.exe will hold in a
variable, so a manual command line cannot carry it. Nothing is generated into `target/`, so
`mvn clean` cannot break the script.

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

DuckDB JDBC is `2.0.0.1` (note the **4-part** version, unlike most artifacts). It must line up
with the Python `duckdb` package the Streamlit frontend uses: a newer driver can read an older
file, but an older driver cannot read a newer file. `requirements.txt` currently says
`duckdb>=1.0.0`, so install 2.0.x there (`pip install "duckdb~=2.0.0"`) to match the Java side.
If the frontend fails with a "file format version" error, align these two.

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
|   |-- schema.sql            # CREATE TABLE IF NOT EXISTS, all six tables
|   `-- logback.xml           # Flink and pekko at WARN; without it Logback defaults to root DEBUG
`-- java/com/fyp/
    |-- AttributionJob.java           # main(): run the pipeline, print the results
    |-- AttributionService.java       # run(csv) -> runId, fetchAll(runId) -> RunResults
    |-- RunContext.java               # runId, startedAt, dataset, config snapshot
    |-- ToolchainSmokeCheck.java      # environment gate; delete once you no longer need it
    |-- HelloWorld.java, Main.java    # scratch samples from the JDK setup, not part of the pipeline
    |-- config/               # AppConfig + DatasetConfig/FlinkConfig/PipelineConfig records, ConfigLoader
    |-- model/                # Tweet, UnifiedEvent, Attribution, WindowOutput
    |-- pipeline/
    |   |-- loader/           # CsvDatasetLoader -> List<Tweet>
    |   |-- flink/            # HashtagExtractor, EventTimestampAssigner, CategoryAttributionFunction
    |   `-- sink/             # DuckDbSink + DuckDbSinkWriter (Sink V2, batch 100)
    `-- storage/              # SqlExecutor (interface), DuckDbExecutor, SchemaInitializer,
                              # RunRepository, WindowOutputRepository, Row, RunResults
```

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

:: the environment gate
run.bat smoke

:: the tests
run.bat test

:: delete backend\target
run.bat clean
```

`run.bat` takes exactly one word and dispatches on it; it does not forward extra arguments. For the
Maven goals underneath, call Maven yourself from `backend/`. The pom reads the `mainClass` property,
not `exec.mainClass`:

```cmd
mvn compile exec:exec "-DmainClass=com.fyp.ToolchainSmokeCheck"

:: Submit to a real cluster
flink run -c com.fyp.AttributionJob target\fyp-attribution-backend-0.1.0.jar
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

The shaded jar also runs standalone, which is the quickest way to prove a packaging regression:

```cmd
mvn -q package
java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED ^
     -jar target\fyp-attribution-backend-0.1.0.jar
```

Three things to know:

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

48 tests: pure ones for config, hashtag extraction, CSV loading and attribution arithmetic; MiniCluster
ones for the window firing; `RecordingExecutor` ones for batch shape and SQL; and
`AttributionPipelineEndToEndTest`, which runs CSV to a real DuckDB file in a temp directory twice to
prove two runs coexist. `flink-test-utils` gives the MiniCluster, so no Kafka and no cluster needed.
