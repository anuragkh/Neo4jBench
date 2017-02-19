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
  static int numWarmupQueries, numMeasureQueries;
  static int numClients;
  static boolean tuned;
  static String pageCache;
  static String query;

  public static void main(String[] args) {
    dbPath = args[0];
    String queryFile = args[1];
    outputFile = queryFile + ".result";
    numWarmupQueries = Integer.parseInt(args[2]);
    numMeasureQueries = Integer.parseInt(args[3]);
    numClients = Integer.parseInt(args[4]);
    tuned = Boolean.valueOf(args[5]);
    pageCache = args[6];

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
    if (tuned) {
      db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
        .setConfig(GraphDatabaseSettings.cache_type, "none")
        .setConfig(GraphDatabaseSettings.pagecache_memory, pageCache).newGraphDatabase();
    } else {
      db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    }
    BenchUtils.registerShutdownHook(db);
    System.out.println("Done opening");

    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
    System.out.println("Testing query for timeout...");
    Transaction tx = db.beginTx();
    int ret = PathQuery.runTimed(db, query);
    if (ret < 0) {
      tx.failure();
      tx.close();

      out.println("DNF");
      out.close();
      System.out.println("Query did not finish: " + query);
      System.exit(-1);
    }
    tx.success();
    tx.close();

    System.out.println("Executing query " + query);
    tx = db.beginTx();
    long queryStart = System.nanoTime();
    int count = PathQuery.run(db, query);
    long queryEnd = System.nanoTime();
    double totTime = (queryEnd - queryStart) / ((double) 1000);
    out.println(count + "\t" + totTime);
    tx.success();
    tx.close();
    out.close();
  }
}
