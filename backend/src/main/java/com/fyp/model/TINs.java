package com.fyp.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TIN prototype: the four tables of docs/design/reference_schema.md modelled in memory, mock
 * datasets pushed through them, and the §9 invariants checked on what comes out.
 *
 * <p>One source per dataset, and each source brings the map registered for it. The chains are
 * therefore identical in structure — csv, map, countable records — and they meet at the window, which
 * is the only place their numbers are combined. Adding an experiment means adding one entry to
 * {@link #SOURCES}: the rows of a source, and the extractor that knows its columns.
 *
 * <p>Deliberately free of Flink: the attribution rules being explored here are per-operator, not
 * per-runtime, and a window can be computed with one modulo while that is still true.
 *
 * <p>Run from {@code backend/} with
 * {@code mvn compile exec:exec "-DmainClass=com.fyp.model.TINs"}.
 */
public final class TINs {

    private static final Pattern HASHTAG = Pattern.compile("#(\\w+)", Pattern.UNICODE_CHARACTER_CLASS);

    private static final long WINDOW_SIZE_MS = 60_000L;

    private static final String RUN_ID = "run-prototype";

    /**
     * The configurable seam: which source is read, and which map turns its rows into countable
     * records. In the job this is the dataset entry in config.yaml, one entry per source.
     *
     * <p>Insertion order is kept on purpose, because the order decides which source is minted as K1
     * and which as K2. An unstable order would rename the vertices between runs, and rows from two
     * runs could no longer be compared.
     */
    private static final Map<String, SourceSpec> SOURCES = buildSources();

    private static Map<String, SourceSpec> buildSources() {
        Map<String, SourceSpec> sources = new LinkedHashMap<>();
        sources.put("tweet", new SourceSpec(new HashtagCategories(), tweetRows()));
        sources.put("comment", new SourceSpec(new ColumnCategories("topic"), commentRows()));
        return Collections.unmodifiableMap(sources);
    }

    // Values: immutable, compared and printed by value.
    private record Window(long start, long end) implements Serializable {}

    private record Vertex(String id, String description) implements Serializable {}

    private record Edge(String id, Vertex source, Vertex destination) implements Serializable {}

    private record Interaction(Edge path, String category, Window window, long timestamp, long quantity,
            Attribution attribution) {}

    private record State(Vertex vertex, Window window, String category, long quantity,
            Attribution attribution, long firedAt) {}

    /** A registered source: the map its rows need, and the rows themselves. */
    private record SourceSpec(EventExtractor extractor, List<RawRow> rows) {}

    /**
     * One row of a csv, read by column name so that one loader can serve every dataset.
     *
     * <p>The contract between datasets is two columns: every source carries {@code user_id} and
     * {@code timestamp}, because the attribution and the window are built from them. Everything else
     * belongs to the extractor, which is the only way two sources can differ and still share a window.
     *
     * <p>Columns are sorted so that a row prints identically every run, the same reason
     * {@code Attribution} sorts its keys.
     */
    private record RawRow(Map<String, String> fields) {

        RawRow {
            fields = Collections.unmodifiableMap(new TreeMap<>(fields));
        }

        String get(String column) {
            String value = fields.get(column);
            if (value == null) {
                throw new IllegalStateException("No '" + column + "' column in row " + fields.keySet());
            }
            return value;
        }

        long timestamp() {
            return Long.parseLong(get("timestamp"));
        }

        /** The columns that say what the row is about, which is where the two datasets part company. */
        String subject() {
            List<String> parts = new ArrayList<>();
            for (Map.Entry<String, String> column : fields.entrySet()) {
                if (!column.getKey().equals("user_id") && !column.getKey().equals("timestamp")) {
                    parts.add(column.getKey() + "=" + column.getValue());
                }
            }
            return String.join(" ", parts);
        }
    }

    /**
     * The countable record the map emits: one category of one row, and how many times that row named
     * it. A tweet carrying "#AI #AI #PYTHON" produces two of these, with quantities 2 and 1, not three
     * records of quantity 1.
     *
     * <p>This is the type every source produces, which is what lets one window aggregate over all of
     * them without knowing what it is holding.
     */
    private record Countable(String source, String category, long timestamp, long quantity) {}

    /**
     * One source chain, with the two vertices and the two edges it owns. Bundled so a window group can
     * say which vertex its numbers arrived through instead of assuming there is only one chain.
     */
    private record SourceStage(String name, Vertex source, Vertex map, Edge sourceToMap, Edge mapToWindow,
            EventExtractor extractor, List<RawRow> rows) {}

    /** The part that changes per dataset: which categories one row contributes. */
    private interface EventExtractor {
        List<String> categoriesOf(RawRow row);
    }

    /** Tweets: the categories are the hashtags typed into the body. */
    private static final class HashtagCategories implements EventExtractor {

        @Override
        public List<String> categoriesOf(RawRow row) {
            return hashtagsOf(row.get("text"));
        }
    }

    /** Comments today, NYC taxi and NEXMark tomorrow: the category is already a column. */
    private static final class ColumnCategories implements EventExtractor {

        private final String column;

        ColumnCategories(String column) {
            this.column = column;
        }

        @Override
        public List<String> categoriesOf(RawRow row) {
            String value = row.get(column);
            return value.isBlank() ? List.of() : List.of(value.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * One (category, window) pair and everything that fell into it, kept per source chain. Mutable on
     * purpose: it is filled record by record, then frozen into the attributions.
     */
    private static final class WindowGroup {
        private final String category;
        private final Window window;
        private final Map<String, Map<String, Long>> recordsByChain = new TreeMap<>();
        private final Map<String, Map<String, Long>> eventsByChain = new TreeMap<>();

        WindowGroup(String category, Window window) {
            this.category = category;
            this.window = window;
        }

        void accept(SourceStage chain, Countable record) {
            recordsByChain.computeIfAbsent(chain.map().id(), k -> new TreeMap<>())
                    .merge(record.source(), 1L, Long::sum);
            eventsByChain.computeIfAbsent(chain.map().id(), k -> new TreeMap<>())
                    .merge(record.source(), record.quantity(), Long::sum);
        }

        /** Step 4: take in a group covering the same (category, window), keeping the chains apart inside. */
        void absorb(WindowGroup other) {
            absorbAll(recordsByChain, other.recordsByChain);
            absorbAll(eventsByChain, other.eventsByChain);
        }

        private static void absorbAll(Map<String, Map<String, Long>> into,
                Map<String, Map<String, Long>> incoming) {
            incoming.forEach((chainId, perSource) -> {
                Map<String, Long> target = into.computeIfAbsent(chainId, k -> new TreeMap<>());
                perSource.forEach((source, quantity) -> target.merge(source, quantity, Long::sum));
            });
        }

        Set<String> contributedBy() {
            return eventsByChain.keySet();
        }

        Attribution recordsFrom(String chainId) {
            return new Attribution(recordsByChain.getOrDefault(chainId, Map.of()));
        }

        Attribution eventsFrom(String chainId) {
            return new Attribution(eventsByChain.getOrDefault(chainId, Map.of()));
        }

        /** What the window holds when every chain's contribution is combined, which is §1's merge rule. */
        Attribution eventsTotal() {
            Map<String, Long> total = new TreeMap<>();
            for (Map<String, Long> perSource : eventsByChain.values()) {
                perSource.forEach((source, quantity) -> total.merge(source, quantity, Long::sum));
            }
            return new Attribution(total);
        }
    }

    // Class: it accumulates, so it cannot be a record.
    private static final class Graph {
        private final Map<String, Vertex> byId = new TreeMap<>();
        private final List<Edge> edges = new ArrayList<>();
        private final Map<String, Integer> counter = new HashMap<>();

        Vertex vertex(String type, String description) {
            int n = counter.merge(type, 1, Integer::sum);
            Vertex v = new Vertex(type + n, description);
            byId.put(v.id(), v);
            return v;
        }

        Edge connect(Vertex from, Vertex to) {
            Edge e = new Edge(from.id() + "->" + to.id(), from, to);
            edges.add(e);
            return e;
        }

        List<Vertex> vertices() {
            return List.copyOf(byId.values());
        }

        List<Edge> edges() {
            return List.copyOf(edges);
        }
    }

    public static void main(String[] args) {
        Graph graph = new Graph();

        // 1. Set 1 to N sources.
        // W1 and S1 exist once no matter how many datasets are registered; each dataset then adds its
        // own K and M, so this loop IS the topology and adding a third dataset cannot reach downstream.
        Vertex w1 = graph.vertex("W", "60s tumbling count, keyed by category");
        Vertex s1 = graph.vertex("S", "duckdb sink");
        Edge w1s1 = graph.connect(w1, s1);
        List<SourceStage> chains = new ArrayList<>();
        for (Map.Entry<String, SourceSpec> entry : SOURCES.entrySet()) {
            String name = entry.getKey();
            SourceSpec spec = entry.getValue();
            Vertex source = graph.vertex("K", name + " csv (" + spec.rows().size() + " rows)");
            Vertex map = graph.vertex("M", name + " rows to categories, one record each");
            Edge sourceToMap = graph.connect(source, map);
            Edge mapToWindow = graph.connect(map, w1);
            chains.add(new SourceStage(name, source, map, sourceToMap, mapToWindow, spec.extractor(),
                    spec.rows()));
        }

        // 2. Reduce each stream on its own, keyed by the map that produced it.
        Map<String, List<Countable>> recordsByChain = new LinkedHashMap<>();
        for (SourceStage chain : chains) {
            List<Countable> records = new ArrayList<>();
            for (RawRow row : chain.rows()) {
                records.addAll(toCountables(row, chain.extractor()));
            }
            recordsByChain.put(chain.map().id(), records);
        }

        // 3. Group each chain's records into windows, one set of groups per chain.
        Map<String, List<WindowGroup>> groupsByChain = new LinkedHashMap<>();
        for (SourceStage chain : chains) {
            groupsByChain.put(chain.map().id(),
                    groupByWindow(chain, recordsByChain.get(chain.map().id())));
        }

        // 4. Merge the per-chain groups. This is a fold over already finished groups, not a second
        // window operator, so it cannot fire a window the two chains disagree about: both derived
        // theirs from the same modulo over event time.
        List<WindowGroup> groups = mergeGroups(groupsByChain);

        List<Interaction> interactions = new ArrayList<>();
        List<State> states = new ArrayList<>();
        for (WindowGroup group : groups) {
            long firedAt = group.window.end();
            for (SourceStage chain : chains) {
                if (!group.contributedBy().contains(chain.map().id())) {
                    continue;
                }
                Attribution records = group.recordsFrom(chain.map().id());
                Attribution events = group.eventsFrom(chain.map().id());
                interactions.add(new Interaction(chain.sourceToMap(), group.category, group.window, firedAt,
                        records.total(), records));
                interactions.add(new Interaction(chain.mapToWindow(), group.category, group.window, firedAt,
                        events.total(), events));
            }
            Attribution total = group.eventsTotal();
            interactions.add(new Interaction(w1s1, group.category, group.window, firedAt, total.total(), total));
            states.add(new State(w1, group.window, group.category, total.total(), total, firedAt));
        }

        printGraph(graph);
        printMap(chains);
        printRows(interactions, states);
        checkInvariants(chains, interactions, states);
    }

    /**
     * Step 3: one chain's records fall into the windows they belong to. Keyed by (category, window)
     * only, because the chain is fixed for the whole call, so every group here was reached through
     * one map vertex.
     */
    private static List<WindowGroup> groupByWindow(SourceStage chain, List<Countable> records) {
        Map<String, WindowGroup> groups = new TreeMap<>();
        for (Countable record : records) {
            Window window = windowOf(record.timestamp());
            groups.computeIfAbsent(record.category() + "@" + window.start(),
                    k -> new WindowGroup(record.category(), window)).accept(chain, record);
        }
        return new ArrayList<>(groups.values());
    }

    /**
     * Step 4: the chains meet. Two groups holding the same (category, window) become one, and each
     * chain keeps its own numbers inside it, so a reader can still tell what arrived through M1 from
     * what arrived through M2.
     */
    private static List<WindowGroup> mergeGroups(Map<String, List<WindowGroup>> groupsByChain) {
        Map<String, WindowGroup> merged = new TreeMap<>();
        for (List<WindowGroup> chainGroups : groupsByChain.values()) {
            for (WindowGroup group : chainGroups) {
                merged.computeIfAbsent(group.category + "@" + group.window.start(),
                        k -> new WindowGroup(group.category, group.window)).absorb(group);
            }
        }
        return new ArrayList<>(merged.values());
    }

    /**
     * The map: one row in, one countable record per distinct category it contributes. Where the
     * categories come from is the extractor's business, so the record shape and the repeat collapsing
     * live here and no extractor has to remember either.
     */
    private static List<Countable> toCountables(RawRow row, EventExtractor extractor) {
        Map<String, Long> repeats = new TreeMap<>();
        for (String category : extractor.categoriesOf(row)) {
            repeats.merge(category, 1L, Long::sum);
        }
        List<Countable> records = new ArrayList<>();
        for (Map.Entry<String, Long> entry : repeats.entrySet()) {
            records.add(new Countable(row.get("user_id"), entry.getKey(), row.timestamp(), entry.getValue()));
        }
        return records;
    }

    /** The tumbling window one event falls into, aligned to the epoch like TumblingEventTimeWindows. */
    private static Window windowOf(long timestamp) {
        long start = timestamp - Math.floorMod(timestamp, WINDOW_SIZE_MS);
        return new Window(start, start + WINDOW_SIZE_MS);
    }

    private static List<String> hashtagsOf(String text) {
        List<String> tags = new ArrayList<>();
        Matcher matcher = HASHTAG.matcher(text);
        while (matcher.find()) {
            tags.add(matcher.group(1).toUpperCase(Locale.ROOT));
        }
        return tags;
    }

    private static List<RawRow> tweetRows() {
        long base = 1_735_696_800_000L;
        return List.of(
                tweet("user_1", "Loving #AI and #DATA", base),
                tweet("user_2", "#AI is awesome", base + 10_000L),
                tweet("user_3", "#UST today", base + 20_000L),
                tweet("user_1", "#AI again #AI twice", base + 30_000L),
                tweet("user_2", "#DATA and then #AI", base + 70_000L),
                tweet("user_1", "#AI", base + 90_000L));
    }

    private static List<RawRow> commentRows() {
        long base = 1_735_696_800_000L;
        return List.of(
                comment("user_2", "AI", base + 15_000L),
                comment("user_4", "AI", base + 80_000L));
    }

    private static RawRow tweet(String userId, String text, long timestamp) {
        return new RawRow(Map.of("user_id", userId, "text", text, "timestamp", Long.toString(timestamp)));
    }

    private static RawRow comment(String userId, String topic, long timestamp) {
        return new RawRow(Map.of("user_id", userId, "topic", topic, "timestamp", Long.toString(timestamp)));
    }

    private static void printGraph(Graph graph) {
        System.out.println("== tin_vertices ==");
        for (Vertex v : graph.vertices()) {
            System.out.printf("%-4s | %-8s | %s%n", RUN_ID, v.id(), v.description());
        }
        System.out.println();
        System.out.println("== tin_edges ==");
        for (Edge e : graph.edges()) {
            System.out.printf("%s -> %s%n", e.source().id(), e.destination().id());
        }
        System.out.println();
    }

    private static void printMap(List<SourceStage> chains) {
        System.out.println("== the maps: one row in, one countable record per distinct category ==");
        for (SourceStage chain : chains) {
            System.out.printf("-- %s reads %s, %s maps its rows%n", chain.map().id(), chain.source().id(),
                    chain.extractor().getClass().getSimpleName());
            for (RawRow row : chain.rows()) {
                List<String> records = new ArrayList<>();
                for (Countable record : toCountables(row, chain.extractor())) {
                    records.add("#" + record.category() + " q=" + record.quantity());
                }
                System.out.printf("%-4s %-8s %-26s -> %s%n", chain.map().id(), row.get("user_id"),
                        row.subject(), String.join("  ", records));
            }
        }
        System.out.println();
    }

    private static void printRows(List<Interaction> interactions, List<State> states) {
        System.out.println("== tin_interactions ==");
        for (Interaction i : interactions) {
            System.out.printf("%s | %s | %s | %-14s | qty=%-3d | %s%n", RUN_ID, i.path().source().id(),
                    i.path().destination().id(), i.category() + "@" + i.window().start(), i.quantity(),
                    i.attribution().toJson());
        }
        System.out.println();
        System.out.println("== tin_states ==");
        for (State s : states) {
            System.out.printf("%s | %s | [%s, %s) | %-6s | qty=%-3d | fired_at=%s | %s%n", RUN_ID,
                    s.vertex().id(), s.window().start(), s.window().end(), s.category(), s.quantity(),
                    s.firedAt(), s.attribution().toJson());
        }
        System.out.println();
    }

    private static void checkInvariants(List<SourceStage> chains, List<Interaction> interactions,
            List<State> states) {
        System.out.println("== invariants (reference_schema.md section 9) ==");
        boolean conserved = true;
        for (Interaction i : interactions) {
            conserved &= i.attribution().total() == i.quantity();
        }
        for (State s : states) {
            conserved &= s.attribution().total() == s.quantity();
        }
        report("quantity conservation", conserved, "sum(attribution.values()) == quantity on every row");

        Set<String> distinct = new HashSet<>();
        for (Interaction i : interactions) {
            distinct.add(i.path().id() + "#" + i.category() + "@" + i.window().start());
        }
        report("write once", distinct.size() == interactions.size(),
                interactions.size() + " interactions, " + distinct.size() + " distinct (edge, category, window)");

        int rows = 0;
        boolean collapsed = true;
        Set<String> mapIds = new HashSet<>();
        for (SourceStage chain : chains) {
            mapIds.add(chain.map().id());
            rows += chain.rows().size();
            for (RawRow row : chain.rows()) {
                List<Countable> mapped = toCountables(row, chain.extractor());
                List<String> named = chain.extractor().categoriesOf(row);
                long carried = mapped.stream().mapToLong(Countable::quantity).sum();
                collapsed &= mapped.size() == new HashSet<>(named).size() && carried == named.size();
            }
        }

        long records = 0;
        long events = 0;
        long forwarded = 0;
        for (Interaction i : interactions) {
            String destination = i.path().destination().id();
            if (mapIds.contains(destination)) {
                records += i.quantity();
            } else if (destination.equals("W1")) {
                events += i.quantity();
            }
            if (i.path().source().id().equals("W1")) {
                forwarded += i.quantity();
            }
        }
        report("every event reaches the window", records <= events,
                rows + " rows -> " + records + " countable records -> " + events + " events");
        report("map collapses repeats", collapsed,
                "one record per distinct category, quantity is how often the row named it");
        report("the window leaks nothing", events == forwarded,
                events + " events entered W1 from " + chains.size() + " chains, " + forwarded + " left it");

        long windowCount = states.stream().map(s -> s.window().start()).distinct().count();
        System.out.printf("windows fired: %d, (category, window) results: %d%n", windowCount, states.size());
    }

    private static void report(String name, boolean ok, String detail) {
        System.out.printf("[%s] %-28s %s%n", ok ? "PASS" : "FAIL", name, detail);
    }
}
