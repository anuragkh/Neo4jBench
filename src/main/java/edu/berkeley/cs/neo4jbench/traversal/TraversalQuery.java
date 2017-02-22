package edu.berkeley.cs.neo4jbench.traversal;

import edu.berkeley.cs.neo4jbench.tao.AType;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import java.util.ArrayList;
import java.util.List;

public class TraversalQuery {

  private static final RelationshipType[] relationship;

  static {
    relationship = new RelationshipType[5];
    // assume 5 atypes
    for (int i = 0; i < 5; ++i) {
      relationship[i] = new AType(String.valueOf(i));
    }
  }

  public static List<Long> traverse(GraphDatabaseService db, long startId, TraversalType type) {
    TraversalDescription search;
    Node startNode = db.getNodeById(startId);
    if (type == TraversalType.BFS) {
      search =
        db.traversalDescription().breadthFirst().relationships(relationship[0], Direction.OUTGOING)
          .evaluator(Evaluators.toDepth(5));
    } else if (type == TraversalType.DFS) {
      search =
        db.traversalDescription().depthFirst().relationships(relationship[0], Direction.OUTGOING)
          .evaluator(Evaluators.toDepth(5));
    } else {
      throw new IllegalArgumentException("Unknown traversal type " + type.toString());
    }

    ArrayList<Long> results = new ArrayList<>();
    if (search != null) {
      Traverser traverser = search.traverse(startNode);
      for (Node node : traverser.nodes()) {
        results.add(node.getId());
      }
    }

    return results;
  }
}
