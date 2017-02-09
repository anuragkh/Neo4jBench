package edu.berkeley.cs.succinctgraph.neo4jbench.path;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

public class PathQuery {

  static int run(GraphDatabaseService db, String query) {
    int count = 0;

    Result result = db.execute(query);
    while (result.hasNext()) {
      result.next();
      count++;
    }

    return count;
  }

}
