package edu.berkeley.cs.neo4jbench.path;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.concurrent.*;

public class PathQuery {

  public static final long TIMEOUT_SECS = 600;

  static int run(GraphDatabaseService db, String query) {
    int count = 0;

    Result result = db.execute(query);
    while (result.hasNext()) {
      result.next();
      count++;
    }

    return count;
  }

  static int runTimed(final GraphDatabaseService db, final String query) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Callable<Integer> task = new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        int count = 0;
        Result result = db.execute(query);
        while (result.hasNext()) {
          result.next();
          count++;
        }
        return count;
      }
    };

    int ret;
    Future<Integer> future = executor.submit(task);
    try {
      ret = future.get(TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      return -3;
    } catch (ExecutionException e) {
      return -2;
    } catch (TimeoutException e) {
      return -1;
    }
    return ret;
  }

}
