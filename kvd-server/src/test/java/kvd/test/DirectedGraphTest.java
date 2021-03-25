/*
 * Copyright 2021 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.BFSShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

public class DirectedGraphTest {

  private <V,E> boolean hasPath(Graph<V,E> graph, V v1, V v2) {
    try {
//      DijkstraShortestPath<V,E> shortestPath = new DijkstraShortestPath<>(graph);
//      BellmanFordShortestPath<V,E> shortestPath = new BellmanFordShortestPath<>(graph);
      BFSShortestPath<V,E> shortestPath = new BFSShortestPath<>(graph);
      GraphPath<V,E> path = shortestPath.getPath(v1, v2);
      return (path != null) && (path.getLength() > 0);
    } catch(IllegalArgumentException e) {
      System.out.println(e.getMessage());
      return false;
    }
  }

  @Test
  public void graphTest() {
//    DefaultDirectedGraph<String, DefaultEdge> directedGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
//    DirectedMultigraph<String, DefaultEdge> directedGraph = new DirectedMultigraph<>(DefaultEdge.class);
    Graph<String, DefaultEdge> directedGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    directedGraph.addVertex("v1");
    directedGraph.addVertex("v2");
    directedGraph.addVertex("v3");
    directedGraph.addVertex("v4");
    directedGraph.addEdge("v1", "v2");
    directedGraph.addEdge("v2", "v3");
    directedGraph.addEdge("v1", "v4");
    assertThrows(IllegalArgumentException.class, () -> directedGraph.addEdge("v4", "v1"));
    assertThrows(IllegalArgumentException.class, () -> directedGraph.addEdge("v1", "v1"));
    assertTrue(hasPath(directedGraph, "v1", "v3"));
    assertFalse(hasPath(directedGraph, "v3", "v1"));
    assertTrue(hasPath(directedGraph, "v1", "v4"));
    assertFalse(hasPath(directedGraph, "v4", "v1"));
    assertFalse(hasPath(directedGraph, "v1", "v1"));
    directedGraph.removeVertex("v2");
    assertFalse(hasPath(directedGraph, "v1", "v3"));
  }

}
