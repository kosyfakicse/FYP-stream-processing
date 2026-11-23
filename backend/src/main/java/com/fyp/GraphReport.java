package com.fyp;

import com.fyp.config.AppConfig;
import com.fyp.config.ConfigLoader;
import com.fyp.model.Edge;
import com.fyp.model.Interaction;
import com.fyp.model.RunGraph;
import com.fyp.model.State;
import com.fyp.model.Vertex;
import com.fyp.service.AttributionService;

/**
 * Prints the network one stored run recorded: its operators, the flows between them, what each operator
 * held over each interval and how much each flow carried. It starts no job, so an id from last week reads
 * back exactly as it was written -- which is the point, since a stored run describes the code that ran
 * then, not the code on disk now.
 *
 * <p>Run from {@code backend/} with {@code run.bat graph <runId>}. The ids come from the first line a
 * {@code run.bat} job prints or from {@code GET /runs}.
 *
 * <p>The four sections are the four {@code tin_*} tables, and the counts on the first line are the
 * quickest way to see how much of the capture a run's own code had: an id recorded before the box capture
 * prints no {@code K1} or {@code M1} state row at all, and one recorded before the interactions were
 * measured per edge prints fewer rows for the same windows.
 */
public final class GraphReport {

    /** Enough ids to paste one back, and few enough that the list is still readable on one screen. */
    private static final int IDS_TO_SUGGEST = 8;

    private GraphReport() {
    }

    public static void main(String[] args) throws Exception {
        // `run.bat graph` hands the id over as a system property, because exec:exec builds its java
        // command line from pom.xml and forwards no program argument. An argument still works from an IDE.
        String runId = args.length > 0 ? args[0] : System.getProperty("runId", "");
        if (runId.isBlank()) {
            System.out.println("usage: run.bat graph <runId>");
            return;
        }

        AppConfig config = ConfigLoader.load();
        try (AttributionService service = AttributionService.fromConfig(config)) {
            // The same startup the API server does, and no row is written by this route. It belongs here
            // because a database that has never been used has no tables to read.
            service.ensureSchema();
            if (service.findRun(runId).isEmpty()) {
                System.out.println("no run with id " + runId + " in " + config.pipeline().duckdbFile());
                service.listRuns().stream().limit(IDS_TO_SUGGEST)
                        .forEach(run -> System.out.println("  " + run.runId()));
                return;
            }
            print(runId, service.findGraph(runId));
        }
    }

    private static void print(String runId, RunGraph graph) {
        System.out.printf("%s: %d vertices, %d edges, %d states, %d interactions%n", runId,
                graph.vertices().size(), graph.edges().size(), graph.states().size(),
                graph.interactions().size());

        System.out.println();
        System.out.println("== tin_vertices ==");
        for (Vertex vertex : graph.vertices()) {
            System.out.printf("%-4s | %-8s | %s%n", vertex.vertexId(), vertex.vertexType(),
                    vertex.description());
        }

        System.out.println();
        System.out.println("== tin_edges ==");
        for (Edge edge : graph.edges()) {
            System.out.printf("%s -> %s%n", edge.sourceVertex(), edge.targetVertex());
        }

        System.out.println();
        System.out.println("== tin_states == what the box held; the K1 and M1 counts are cumulative");
        for (State state : graph.states()) {
            System.out.printf("%-4s | %-8s | [%d, %d) | qty=%-5d | fired_at=%-14d | %s%n",
                    state.vertexId(), asCategory(state.category()), state.timeStart(), state.timeEnd(),
                    state.quantity(), state.firedAt(), state.attribution().toJson());
        }

        System.out.println();
        System.out.println("== tin_interactions == what the edge carried; keys name vertices, never users");
        for (Interaction interaction : graph.interactions()) {
            System.out.printf("%s -> %-4s | %-8s | ts=%-14d | qty=%-5d | %s%n",
                    interaction.sourceVertex(), interaction.targetVertex(),
                    asCategory(interaction.category()), interaction.timestamp(), interaction.quantity(),
                    interaction.attribution().toJson());
        }
    }

    /**
     * The three row kinds whose operator was never keyed by hashtag store a null category. A bare "null"
     * mid-row reads like a broken write, so it prints as a dash.
     */
    private static String asCategory(String category) {
        return category == null ? "-" : category;
    }
}
