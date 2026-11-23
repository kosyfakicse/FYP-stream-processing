package com.fyp.model;

import java.util.List;

/**
 * The network one run recorded: its operators, the flows between them, what each operator held and how
 * much each flow carried.
 *
 * <p>The four lists are the four {@code tin_*} tables filtered to one run id, read back in the order
 * they were written. They are not four fields of one object because they are not four facts about one
 * thing: a vertex is an operator, a state is an operator over an interval, an interaction is an edge over
 * an interval, and an edge says nothing about either. A caller that wants a graph draws vertices from
 * {@link #vertices()} and edges from {@link #edges()}, then labels an interval with the other two.
 *
 * @param vertices the operators the run built, ids minted per run
 * @param edges the logical flows between those operators
 * @param states what each operator held, one row per key and interval
 * @param interactions how much each edge carried, one row per edge and interval
 */
public record RunGraph(List<Vertex> vertices, List<Edge> edges, List<State> states,
        List<Interaction> interactions) {
}
