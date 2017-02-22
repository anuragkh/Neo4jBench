package edu.berkeley.cs.neo4jbench.traversal;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class TraversalBench {
  static String outputFile;
  static String traversalType;
  static String dbPath;
  static String pageCache;
  static boolean tuned;

  public static void main(String[] args) {
    dbPath = args[0];
    outputFile = args[1];
    traversalType = args[2];
    tuned = Boolean.valueOf(args[3]);
    pageCache = args[4];

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
    System.out.println("Done opening");


    System.out.println("Running warmup...");
    for (long i = 100; i < 110; i++) {
      Transaction tx = db.beginTx();
      TraversalQuery.traverse(db, i, TraversalType.fromString(traversalType));
      tx.success();
      tx.close();

    }

    System.out.println("Finished warmup, running measurements...");
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
    for (long i = 0; i < 10; i++) {
      Transaction tx = db.beginTx();
      long start = System.nanoTime();
      List<Long> nodeIds = TraversalQuery.traverse(db, i, TraversalType.fromString(traversalType));
      long end = System.nanoTime();
      tx.success();
      tx.close();
      double totTime = (end - start) / ((double) 1000);

      System.out.println(
        "Finished " + traversalType + " on node " + i + " with " + nodeIds.size() + " results in "
          + totTime + "us");
      out.println(nodeIds.size() + "\t" + totTime);

    }
    out.close();
    System.out.println("Finished measurements.");

    db.shutdown();
    System.out.println("Shutdown database.");
    System.exit(0);
  }
}
