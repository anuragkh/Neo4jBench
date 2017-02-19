package edu.berkeley.cs.neo4jbench.path;

import edu.berkeley.cs.neo4jbench.BenchUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PathBench {
  static String outputFile;
  static String dbPath;
  static String pageCache;
  static String query;

  public static void main(String[] args) {
    dbPath = args[0];
    String queryFile = args[1];
    outputFile = queryFile + ".result";
    pageCache = args[2];

    query = BenchUtils.readPathQuery(queryFile);

    try {
      benchLatency();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void benchLatency() throws IOException {
    GraphDatabaseService db;
    System.out.println("About to open database");
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
      .setConfig(GraphDatabaseSettings.cache_type, "none")
      .setConfig(GraphDatabaseSettings.pagecache_memory, pageCache).newGraphDatabase();
    System.out.println("Done opening");

    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
    System.out.println("Executing query " + query);
    Transaction tx = db.beginTx();
    long start = System.nanoTime();
    int count = PathQuery.runTimed(db, query);
    long end = System.nanoTime();
    if (count < 0) {
      tx.failure();
      tx.close();

      out.println("DNF");
      out.close();
      System.out.println("Query did not finish: " + query);
      System.exit(-1);
    }
    tx.success();
    tx.close();
    double totTime = (end - start) / ((double) 1000);
    out.println(count + "\t" + totTime);
    out.close();
    System.out.println("Finished executing query " + query);
    db.shutdown();
    System.out.println("Shutdown database");
    System.exit(0);
  }
}
