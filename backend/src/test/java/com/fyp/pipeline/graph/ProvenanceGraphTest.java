package com.fyp.pipeline.graph;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fyp.model.Edge;
import com.fyp.model.Vertex;

class ProvenanceGraphTest {

    /** The id a run stores its window results under is minted here, so it is pinned here. */
    @Test
    void mintsOneIdPerOperatorInMintOrder() {
        ProvenanceGraph graph = new ProvenanceGraph("run-1");
        graph.mint(ProvenanceGraph.OperatorKind.SOURCE, "csv dataset ds-1 (4 rows)");
        graph.mint(ProvenanceGraph.OperatorKind.MAP, "hashtag extractor");
        graph.mint(ProvenanceGraph.OperatorKind.WINDOW, "60s tumbling count, keyed by category");
        graph.mint(ProvenanceGraph.OperatorKind.SINK, "duckdb sink");

        assertThat(graph.vertices()).containsExactly(
                new Vertex("K1", "run-1", "source", "csv dataset ds-1 (4 rows)"),
                new Vertex("M1", "run-1", "map", "hashtag extractor"),
                new Vertex("W1", "run-1", "window", "60s tumbling count, keyed by category"),
                new Vertex("S1", "run-1", "sink", "duckdb sink"));
    }

    @Test
    void countsEachOperatorKindSeparately() {
        ProvenanceGraph graph = new ProvenanceGraph("run-1");
        graph.mint(ProvenanceGraph.OperatorKind.SOURCE, "tweets");
        graph.mint(ProvenanceGraph.OperatorKind.MAP, "hashtags");
        graph.mint(ProvenanceGraph.OperatorKind.SOURCE, "comments");
        graph.mint(ProvenanceGraph.OperatorKind.MAP, "topics");

        assertThat(graph.vertices()).extracting(Vertex::vertexId)
                .containsExactly("K1", "M1", "K2", "M2");
    }

    @Test
    void connectsOperatorsIntoEdgesThatCarryTheRunThatBuiltThem() {
        ProvenanceGraph graph = new ProvenanceGraph("run-7");
        Vertex source = graph.mint(ProvenanceGraph.OperatorKind.SOURCE, "csv");
        Vertex window = graph.mint(ProvenanceGraph.OperatorKind.WINDOW, "count");
        graph.connect(source, window);

        assertThat(graph.edges()).containsExactly(new Edge("run-7", "K1", "W1"));
    }
}
