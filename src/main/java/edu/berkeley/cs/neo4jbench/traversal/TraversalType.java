package edu.berkeley.cs.neo4jbench.traversal;

enum TraversalType {
  BFS,
  DFS;

  public static TraversalType fromString(String traversalType) {
    if (traversalType.equalsIgnoreCase("bfs")) {
      return BFS;
    } else if (traversalType.equalsIgnoreCase("dfs")) {
      return DFS;
    } else {
      throw new IllegalArgumentException("Invalid traversal type " + traversalType);
    }
  }
}
