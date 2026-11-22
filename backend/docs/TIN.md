# TIN Capture: intervals, boxes and interactions

What a run writes into the `tin_*` tables, which interval each row covers, and what the
`attribution_json` column means in each of them. `API.md` stays the HTTP contract and
`backend/README.md` keeps the shorter version of the reasoning.

One word for one row: what an edge carried is an **interaction** -- in `tin_interactions`, in the
`Interaction` type, and in every sentence here. An earlier draft said *crossing* and named the operator
`CrossingRecorder`; both are gone, so a commit message or a progress report using that word means the
same rows this document calls interactions.

## The problem this fixed

Everything used to be measured in one place. `Interaction.ofFiring(chain, firing)` received only what
the window operator held when it closed, so all three interaction rows were projections of that one
observation:

| row written then | where its number really came from |
|---|---|
| `M1 -> W1` | the events the window received -- faithful, that edge's traffic *is* the window's element list |
| `W1 -> S1` | the same list again -- the graph is `window -> process -> sinkTo` with nothing between, so it cannot ever differ |
| `K1 -> M1` | distinct `eventId`s among those events -- inferred, and it means "tweets that mentioned this tag", not "tweets that reached the map" |

A tweet with no hashtag produced no event at all, so it appeared in no row: the network recorded one
measurement point wearing four vertex names.

There are now four measurement points. `BoxRecorder` sits on the stream into `K1` and on the stream into
`M1`, counting what each box holds; `InteractionRecorder` sits on the keyed stream into `W1` and counts what
actually reached `W1`; the firing still writes its own `W1 -> S1` row. `K1 -> M1` now counts tweets,
hashtag-free included, which is what makes invariant 4 below assertable at all -- and it is asserted, in
`AttributionPipelineEndToEndTest`, against a fixture whose four tweets make five events.

## Intervals

Three independent knobs, all in seconds, all read from `pipeline` in `config.yaml`:

| key | decides |
|---|---|
| `state_tick_seconds` | how often each vertex's state is written |
| `interaction_tick_seconds` | how often each edge writes its interaction row |
| `window_size_seconds` | how long one trend is -- `W1`'s reporting interval |

**The divisibility rule.** A tick interval must divide the window size. Tumbling intervals are anchored
to the epoch, so `1` and `5` into `60` line up with the window boundary and `7` does not: a 7-second
tick straddles two windows, and the ticks can then no longer be summed into the window's own number.
The check belongs at startup, failing fast with the two values in the message, because a mismatch is
invisible in the data and only shows up as an invariant that will not close.

**They stay independent.** Nothing forces `state_tick_seconds` and `interaction_tick_seconds` to be
equal. Making them differ is how you record a lifecycle finely and a handoff coarsely, and the sweep
plan is exactly a sweep over these three.

**What a finer tick buys today, and what it does not.** `K1`'s and `M1`'s boxes and the `M1 -> W1`
interactions are already per-interval -- `BoxRecorderTest` runs two ticks and reads the cumulative box
against the per-tick delta. `W1` is not: a Flink window function only sees its bucket when the bucket
closes, so `state_tick_seconds` below `window_size_seconds` still yields one `W1` row per
(window, category). Per-tick `W1` snapshots need the window stage rebuilt as a `KeyedProcessFunction`
with its own timers, which is deferred; the shipped `config.yaml` sets all three to 60 so nothing looks
finer than it is.

**Where the rule is enforced.** `PipelineConfig.validateTicks()`, called by `ConfigLoader.load` -- the
one reader both `run.bat` and `POST /runs` go through -- so a straddling pair is refused before a job is
built, with both values in the message. An absent key binds to 0 and is reported as "must be positive"
rather than as a divisibility oddity.

## Two kinds of row

> A **state** is what is inside a box at a moment. An **interaction** is what moved between two boxes.

Each vertex's box is whatever that operator really holds, so the state is never a made-up number:

| vertex | its box, and so its state |
|---|---|
| `K1` | rows read from the dataset so far -- cumulative, written by `BoxRecorder` each state tick |
| `M1` | events emitted so far -- cumulative, same recorder |
| `W1` | what the closed window reported, one row per (window, category) |
| `S1` | the sink's buffer, reset at every flush -- not written, see open decisions |

Interactions are the deltas between those boxes: one row per edge per interval, quantity = what the
edge carried.

**When a row exists: only where the box changed.** Decided 2026-11-22 as "snapshot when changed,
box-wise", which Flink gives for free -- it does not call a window function for an interval with no
elements -- so an idle minute writes nothing instead of a row per vertex per second. It costs one
reading rule in exchange: the absence of a row means "unchanged", not "empty", so a box's history is
its last row plus the gaps, not a dense series.

This split is what keeps the two tables from restating each other. If both held "what passed in this
second", `K1`'s state row would be the `K1 -> M1` interaction row under a different key, and the
unique keys would let it through without ever saying anything new.

## What `attribution_json` holds

**The rule: keys follow the box.** A state's map is whatever that operator actually accumulates, and an
interaction's map names the vertex on the other side of the edge. No table gets a key domain because it
has a column that could hold one.

| row | what its box really is | keys | example |
|---|---|---|---|
| state `K1` | a count of rows read | none -- `quantity` carries it | `{}` |
| state `M1` | stateless, holds nothing | none | `{}` |
| state `W1` | the open window's events, reduced per contributor | **users** | `{"user_1":3}` |
| state `S1` | buffered window results, keyed by (window, category) | deferred | -- |
| interaction | what an edge carried, and from which vertex | **vertex** | `{"K1":2}` |
| `window_outputs` | the reported answer | **users** | `{"user_1":3,"user_2":1}` |

The `K1` and `M1` rows store no map at all, deliberately: those operators never held a per-user tally, so
a user map on their row would be data the pipeline did not have. It is also what keeps the propagation
rules meaningful. Source init, Map preserve, Filter pass-or-drop, Window sum, Branch copy-to-all and
Merge combine only say something when a map of *origins* survives passing through an operator. Make every map
name vertices and `M1` "preserving" `{"K1":2}` is an identity on a number already in the `quantity`
column, while the traversal finds no person anywhere upstream of `window_outputs`.

In a single-source chain `{"K1":2}` adds nothing beyond `source_vertex` plus `quantity`. It is stored
anyway because it is the shape that says something the day `M1` has both the csv and a kafka feed.

### The word "source" means two things in this schema

| place | means |
|---|---|
| `Attribution.sourceQuantities` on a `window_outputs` or `W1` state row | a **user id** |
| `Attribution.sourceQuantities` on a `tin_interactions` row | an **operator** -- `K1` |
| `tin_interactions.source_vertex` | an **operator** -- `K1` |

This collision is what made two people describing the same column past each other, so say which sense in
any write-up. The first fix landed: the event field that held a user id is `UnifiedEvent.contributor`
now, not `source`, so no Java field in the pipeline means "user" while spelled like an operator. What is
left is `Attribution.sourceQuantities`, which is one map type serving both senses -- users on a result or
a `W1` box, vertices on an interaction -- and it is named after the *stored* column sense. Renaming it is
possible without touching any response, because `RunController` puts the map into JSON under the key
`attribution`; it is listed under Open decisions rather than done quietly.

### Vertex keys did not make `category` unnecessary

The map answers *who fed this box*; the column answers *which hashtag this row is about*. Different
questions, both needed. `W1`'s box is per hashtag because the job is `keyBy(category)` ahead of the
window, so Flink holds one accumulator per key -- and the same is true of the two edges that carry those
keyed events.

| row | about one hashtag? |
|---|---|
| state `K1`, state `M1` | no -- a tweet is not a tag |
| interaction `K1 -> M1` | no |
| interaction `M1 -> W1`, `W1 -> S1` | yes |
| state `W1` | yes |

Dropping the column would merge two live boxes into one row at the same tick, because
`UNIQUE (run_id, vertex_id, category, time_start, time_end)` then has nothing to separate them: `PYTHON`
qty 3 `{"user_1":3}` and `JAVA` qty 1 `{"user_2":1}` become a single qty 4 box that matches no
`window_outputs` row and breaks invariant 3. So every category-bearing row kind requires it, and the
three tag-blind ones -- `K1`'s box, `M1`'s box and the `K1 -> M1` interaction -- are what the nullable column
exists for.

## Worked example: the fixture `AttributionPipelineEndToEndTest` runs

Four tweets, all inside one 1-second window: `user_1` `#python`, `user_2` `#python`, `user_3` `#java`,
`user_4` `#python and #streaming`. Four rows read, five events emitted, three categories fired. This is
the shipped shape, where every tick equals the window; the numbers below are the ones that test asserts.

```
tin_states
  (K1, null,      1000,2000) qty 4 {}                                      rows read, no tag, no users
  (M1, null,      1000,2000) qty 5 {}                                      events emitted
  (W1, JAVA,      1000,2000) qty 1 {"user_3":1}
  (W1, PYTHON,    1000,2000) qty 3 {"user_1":1,"user_2":1,"user_4":1}
  (W1, STREAMING, 1000,2000) qty 1 {"user_4":1}

tin_interactions                                        timestamp is the interval's end
  (K1->M1, null,      2000) qty 4 {"K1":4}                                  tweets, the 4th naming two tags
  (M1->W1, JAVA,      2000) qty 1 {"M1":1}
  (M1->W1, PYTHON,    2000) qty 3 {"M1":3}
  (M1->W1, STREAMING, 2000) qty 1 {"M1":1}
  (W1->S1, JAVA,      2000) qty 1 {"W1":1}
  (W1->S1, PYTHON,    2000) qty 3 {"W1":3}
  (W1->S1, STREAMING, 2000) qty 1 {"W1":1}
```

The 4-against-5 gap is the whole point of measuring at both ends: `user_4` went through `K1` once and `M1`
twice, and the only way to see that is to count the two streams separately. `null` is the category of a
row whose operator was never keyed by hashtag -- the column is nullable for exactly these three rows, and
`window_outputs` keeps its `NOT NULL`.

Two more things to notice. `W1`'s quantity counts *events*, so a tweet that names the same tag twice
contributes two to it and one to `K1` -- `AttributionWindowJobTest` pins that case. And the firing no
longer carries a tweet count of its own: it used to arrive at the sink inside a `FiredWindow` wrapper that
held both the result and a second, users-only tally, which was a worse reading of the same number now that
`K1` counts tweets where the tweets actually are. The wrapper is deleted and the window emits a plain
`WindowOutput`.

## Worked example: the two-tick fixture `ProvenanceInvariantTest` runs

Five tweets over a 4-second window whose two ticks are 1 second each, so three tweets fall in
`[1000, 2000)` and two in `[2000, 3000)` and `user_4` again names two tags. Everything below is the same
kind of row as above, only repeated -- which is the first time the tables say anything a single interval
cannot.

```
tin_states                                                  boxes are cumulative, windows are not
  (K1, null, 1000,2000) qty 3 {}        (K1, null, 2000,3000) qty 5
  (M1, null, 1000,2000) qty 3 {}        (M1, null, 2000,3000) qty 6
  (W1, JAVA,      0,4000) qty 2 {"user_3":1,"user_5":1}
  (W1, PYTHON,    0,4000) qty 3 {"user_1":1,"user_2":1,"user_4":1}
  (W1, STREAMING, 0,4000) qty 1 {"user_4":1}

tin_interactions
  (K1->M1, null,      2000) qty 3 {"K1":3}     (K1->M1, null,      3000) qty 2 {"K1":2}
  (M1->W1, JAVA,      2000) qty 1 {"M1":1}     (M1->W1, JAVA,      3000) qty 1 {"M1":1}
  (M1->W1, PYTHON,    2000) qty 2 {"M1":2}     (M1->W1, PYTHON,    3000) qty 1 {"M1":1}
  (M1->W1, STREAMING, 3000) qty 1 {"M1":1}
  (W1->S1, JAVA,      4000) qty 2 {"W1":2}     (W1->S1, PYTHON,    4000) qty 3 {"W1":3}
  (W1->S1, STREAMING, 4000) qty 1 {"W1":1}
```

The three checks this makes possible: `K1`'s box grows 3 then 2, which is exactly its two edge rows;
`M1`'s grows 3 then 3, which is *not* its incoming edge (2 tweets in the second interval) and is exactly
the sum of its three outgoing ones; and `PYTHON`'s count of 3 is 2 events from one tick plus 1 from the
next, so the edge total can no longer equal the window total by construction.

## Invariants to assert

These are the reason to write both tables at all -- they make the graph checkable instead of merely
detailed.

1. For each vertex and edge of the same interval: `state(t+1) - state(t) = the interactions over that edge`,
   except where the box resets. This now has both halves to subtract: `BoxRecorder` writes the box and
   the interaction out of it from one count of one stream, which is what makes them agree. **The edge has
   to be the one carrying the box's unit**, so `K1` is checked against `K1 -> M1`, where both count tweets,
   and `M1` against `M1 -> W1`, where both count events. Checking `M1`'s growth against the edge *entering*
   it compares 2 tweets with the 3 events its box gained and fails on a run that is working -- the
   extractor is where the two units part.
2. For each category and window: the sum of the `M1 -> W1` interactions over the window equals that
   window's `window_outputs.output_count`, which is also the total in `window_outputs.attribution_json`.
   The interactions are measured on the edge now rather than copied from the window, so the sum can disagree
   -- which is the point. At the shipped tick it is one row per (window, category); a finer
   `interaction_tick_seconds` makes it a real sum.
3. For each firing: one `W1` state row and one `W1 -> S1` interaction row carry the same quantity --
   they should, since both come from the same firing, and if they ever diverge the sink dropped something.
4. Summed over a whole run, `K1`'s final state equals the csv row count. This used to be unprovable
   because the row did not exist; `AttributionPipelineEndToEndTest` now reads `K1`'s box as 4 over a
   4-row file, and 4 is the number the old capture could not produce for a file with 5 events in it.

`ProvenanceInvariantTest` runs 1 to 3 against the rows a finished job stored, read back through the same
`AttributionService.findGraph` call the API route uses. It carries its own fixture for one reason: the
shipped one has a single interval, where invariant 1 has nothing to subtract and invariant 2 has one term
to add, so both would pass on a measurement that never varied. Its window is 4s with both ticks at 1s, and
five tweets spread across two of those ticks. Invariant 4 stays in `AttributionPipelineEndToEndTest`, where
the csv row count is already known.

Because the two tables use different key domains, checks between them are **quantity-level only**. Never
compare an interaction's `{"M1":3}` against a state's `{"user_1":3}` -- the numbers agree and the keys
mean different things, which is the trap the naming section exists to warn about.

`legacy` and `model/TINs.java` already carry the same style of check in `checkInvariants`.

## What changed on 2026-11-22

| piece | before | now |
|---|---|---|
| where counts are taken | inside `CategoryAttributionFunction` | per operator: `BoxRecorder` at `K1` and at `M1`, `InteractionRecorder` on `M1 -> W1`, the firing for `W1 -> S1` |
| `K1 -> M1` meaning | tweets that mentioned the tag | tweets that arrived, hashtag-free included |
| interaction attribution | users | the contributing vertex |
| states | `W1` only, one per (window, category) | `K1`'s and `M1`'s boxes too, sparse -- no map at either, users at `W1` |
| sink input | `Sink<FiredWindow>` | `Sink<ProvenanceRecord>`, one merged record type, still a single writer |

The sink row was the real cost, and it is paid: `DuckDbSinkWriter` is `SinkWriter<ProvenanceRecord>` and
still the only thing in the job that opens the database -- DuckDB allows one read-write process per file,
the API server is that process during `POST /runs`, and the retry protection is "one flush plus a natural
unique key". Four recorders writing four connections would break both, so `AttributionService.run` builds
four branches over the same two streams and `union(...)`s them into that one sink: `K1`'s box, `M1`'s box,
the per-category `M1 -> W1` interactions and the firings. All three record kinds have a producer now.

The capture has a reader: `AttributionService.findGraph` selects the four tables into a `RunGraph` and
`GET /runs/{runId}/graph` serialises it as `{vertices, edges, states, interactions}`. That shape did not
change, only what the arrays hold -- and two of those changes are client-visible, both written up in
`docs/API.md`: `category` is `null` on the rows that were never keyed by hashtag, and
`interactions[].attribution`'s keys name vertices instead of users. A dashboard that read `attribution` as
"who" has to read the `states` rows and `window_outputs` for that.

There is a second reader with no HTTP in it: `run.bat graph <runId>` runs `com.fyp.GraphReport` over the
same `findGraph` and prints the four tables as columns. Neither reader starts a job or writes a row, so a
stored run reads back exactly as the code of its own day wrote it -- which is the point of printing the
counts on the first line. Because DuckDB allows one process per file, the route refuses to start while
`run.bat serve` holds the handle; with a server up, read the rows over HTTP instead.

Two prerequisites, both settled first:

- ~~`assignTimestampsAndWatermarks` moves before `flatMap`~~ -- **done 2026-11-22.** It is on the source
  stream in `AttributionService.run`, over `Tweet`, and the extractor's output inherits the record
  timestamp, so every stage downstream of `K1` now knows which interval its elements belong to. The
  assigner is `TweetTimestampAssigner`; the event-level one it replaces is no longer referenced.
- **Only non-empty intervals are written** -- and that is the rule the capture ships with. A tick row per
  idle second per vertex would make the row count a function of the dataset's time span instead of its
  size: 4 vertices over one day is roughly 345,000 rows for a 50-tweet run. Flink does not call a window
  function for an interval with no elements, so the rule costs no code.

## What changed on 2026-11-23

The three small things the vocabulary work left behind, done as a set.

- **`EventTimestampAssigner` is deleted** -- the class that was left unreferenced when timestamp assignment
  moved onto the source stream.
- **The firing is a `WindowOutput` again.** `FiredWindow` existed to carry a second, users-only tally beside
  the result. `K1` counts tweets on the stream that still holds whole tweets, so that tally was a duplicate
  reading of a number the graph measures better; `CategoryAttributionFunction`, `ProvenanceRecord`,
  `Interaction.ofFiring` and `DuckDbSinkWriter` now pass the plain result and the type is gone. What the
  wrapper could say -- that two tweets produced three events -- `AttributionWindowJobTest` says about the
  window and `ProvenanceInvariantTest` says about the stored rows.
- **`UnifiedEvent.source` is `contributor`.** It holds a user id, and spelling it `source` is what let two
  readings of `attribution_json` both look correct. Nothing stored or sent changed name: the JSON key is
  `attribution`, and `source_vertex` / `target_vertex` mean operators, which is the sense they keep.

## Open decisions

1. **~~What the two tag-blind row kinds put in `category`.~~ DECIDED 2026-11-22: nullable, and the cost is
   known rather than discovered later.** They said "no need to care about development. it is not deploy we
   still can edit the ddl", so `tin_states.category` and `tin_interactions.category` became nullable and
   `window_outputs.category` stayed `NOT NULL` because every row there is one hashtag's result.
   **Measured, in `ProvenanceRepositoryTest`:** DuckDB treats two null categories as distinct, so a
   repeated tag-blind row is accepted instead of colliding -- the at-least-once guard now covers the
   category-bearing rows only. The fix would be `UNIQUE NULLS NOT DISTINCT`, and `duckdb_jdbc 2.0.0.1`
   rejects that at parse time (`Parser Error: syntax error at or near "NULLS"`), so there was nothing to
   negotiate. A sentinel `"*"` would have kept the guard and was the alternative; the null is the honest
   column and the guard is the thing paid for it.
   Reset consequence, stated once: `CREATE TABLE IF NOT EXISTS` means a file keeps the columns it was
   created with, so a `../data/attribution.duckdb` made before the `category` column landed fails on its
   first tick row with a binder error naming that column. Nothing wrote those tables before this capture,
   so no run that used to work is broken; the file is simply from an older shape, and `run.bat reset` is
   what clears it -- needs the server stopped, and stopping it is their action, not something to do on
   their behalf. **Checked 2026-11-22, so this is not a warning about this checkout:** the live dev file already has `category` on both tick tables,
   nullable, with `UNIQUE (run_id, vertex_id, category, time_start, time_end)` and
   `UNIQUE (run_id, source_vertex, target_vertex, category, timestamp)` and
   `PRIMARY KEY (run_id, vertex_id)` on `tin_vertices` -- read with a read-only JDBC open, no server
   running. No reset is needed before the first run. The rows already in it are the pre-tick shape (6
   states, 18 interactions) and stay that way; `GET /runs/{runId}/graph` for one of those older run ids
   therefore shows three interactions per window and no box rows, which is what was true when it ran. Tests
   are unaffected either way; each opens a temp file.
2. **`S1` state rows** and their fourth attribution domain, deferred above.
3. **Parallelism.** Config says 1, and the box idea is per-operator-instance: at parallelism 2 each
   subtask holds its own box, so state rows need the subtask in the key or the counts are a sum the
   graph cannot attribute. `BoxRecorder`'s two branches already make this concrete -- they key the whole
   stream to one sentinel so a box is one accumulator, and a second subtask would silently halve it.
   Noted, not built.
4. **~~Whether `UnifiedEvent.source` becomes `contributor`.~~ DECIDED 2026-11-23: it is `contributor`.**
   They asked for the three small items as a set, so the rename was approved rather than slipped in. It is a
   user id, and the word was the reason two readings of `attribution_json` both looked correct. It reached
   `event.contributor()` in the window function, the `eventId` construction and the tests, and no column or
   response key changed name.
5. **Whether `Attribution.sourceQuantities` follows.** It is the last place where the schema's two senses of
   *source* share one spelling: users on a `window_outputs` or `W1` state map, vertices on an interaction
   map. Renaming it -- to `perOrigin`, say -- is Java-side only, because `RunController` names the field
   `attribution` in every response, so it is a vocabulary call and not a compatibility one. Not done here,
   because the map is used by both row kinds and the right word depends on which one wins as the network
   grows.

## Deferred: the size sweep

The sweep is over the three intervals, and the shipped fixture cannot support it:
`data/raw/tweet_hashtag.csv` is 50 rows spanning **59 seconds** with 3 users, measured as

| window | (category, window) results fired | events each | mean users per result |
|---|---|---|---|
| 1s | 46 | 1..2 | 1.07 |
| 5s | 24 | 1..5 | 1.50 |
| 7s | 23 | 1..5 | 1.48 |
| 60s | 3 | 7..24 | 2.67 |
| 300s | 3 | 7..24 | 2.67 |

60s and 300s are identical because the whole dataset fits inside one window, so anything at or above the
span is a no-op and the file has to be regenerated over a longer `--start`/`--end` before a sweep means
anything. The 1s row is the interesting result rather than the 60s one: at one second the mean is 1.07
users per window, so "which users caused this hashtag to trend" collapses to "this user posted it".
Finding where that collapse happens is the experiment.
