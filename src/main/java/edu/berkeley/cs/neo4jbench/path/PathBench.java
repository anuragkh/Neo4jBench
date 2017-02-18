package edu.berkeley.cs.neo4jbench.path;

import edu.berkeley.cs.neo4jbench.BenchConstants;
import edu.berkeley.cs.neo4jbench.BenchUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PathBench {
  static String outputFile;
  static String dbPath;
  static int numWarmupQueries, numMeasureQueries;
  static int numClients;
  static boolean tuned;
  static String pageCache;
  static List<String> queries;

  public static void main(String[] args) {
    String type = args[0];
    dbPath = args[1];
    String queryFile = args[2];
    outputFile = args[3];
    numWarmupQueries = Integer.parseInt(args[4]);
    numMeasureQueries = Integer.parseInt(args[5]);
    numClients = Integer.parseInt(args[6]);
    tuned = Boolean.valueOf(args[7]);
    pageCache = args[8];

    queries = new ArrayList<>();
    BenchUtils.readPathQueries(queryFile, queries);

    switch (type) {
      case "latency":
        benchLatency();
        break;
      case "throughput":
        benchThroughput();
        break;
      default:
        System.err.println("Unknown type: " + type);
        break;
    }
  }

  private static void benchLatency() {

    System.out.println("Benchmarking latency for path queries");
    System.out.println("Setting Neo4j's dbms.pagecache.memory: " + pageCache);

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

    BenchUtils.registerShutdownHook(db);
    Transaction tx = db.beginTx();
    try {
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));

      System.out.println("Warming up for " + numWarmupQueries + " queries");
      for (int i = 0; i < numWarmupQueries; ++i) {
        if (i % 10000 == 0) {
          tx.success();
          tx.close();
          tx = db.beginTx();
        }
        String query = queries.get(i);
        if (!query.contains("*"))
          PathQuery.run(db, query);
      }

      System.out.println("Measuring for " + numMeasureQueries + " queries");
      for (int i = 0; i < numMeasureQueries; ++i) {
        if (i % 10000 == 0) {
          tx.success();
          tx.close();
          tx = db.beginTx();
        }
        String query = queries.get(i);
        int count = -1;
        long queryStart = System.nanoTime();
        if (!query.contains("*"))
          count = PathQuery.run(db, query);
        long queryEnd = System.nanoTime();
        double microsecs = (queryEnd - queryStart) / ((double) 1000);
        out.println(count + "\t" + microsecs);
      }
      out.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tx.success();
      tx.close();
      System.out.println("Shutting down database ...");
      db.shutdown();
    }
  }

  private static void benchThroughput() {

    System.out.println("Benchmarking throughput for path queries");
    System.out.println("Setting Neo4j's dbms.pagecache.memory: " + pageCache);

    GraphDatabaseService graphDb;
    System.out.println("About to open database");
    if (tuned) {
      graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
        .setConfig(GraphDatabaseSettings.cache_type, "none")
        .setConfig(GraphDatabaseSettings.pagecache_memory, pageCache).newGraphDatabase();
    } else {
      graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    }
    System.out.println("Done opening");

    BenchUtils.registerShutdownHook(graphDb);
    Transaction tx = null;
    try {
      tx = graphDb.beginTx();
    } finally {
      if (tx != null) {
        tx.success();
        tx.close();
      }
    }

    try {
      List<Thread> clients = new ArrayList<>(numClients);
      for (int i = 0; i < numClients; ++i) {
        clients.add(new Thread(new RunPathThroughput(i, queries, graphDb)));
      }
      for (Thread thread : clients) {
        thread.start();
      }
      for (Thread thread : clients) {
        thread.join();
      }
    } catch (Exception e) {
      System.err.printf("Benchmark throughput exception: %s\n", e);
      System.exit(1);
    } finally {
      BenchUtils.printMemoryFootprint();
      System.out.println("Shutting down database ...");
      graphDb.shutdown();
    }
  }


  static class RunPathThroughput implements Runnable {
    private int clientId;
    private List<String> queries;
    private GraphDatabaseService graphDb;

    public RunPathThroughput(int clientId, List<String> queries, GraphDatabaseService graphDb) {

      this.clientId = clientId;
      this.queries = queries;
      this.graphDb = graphDb;
    }

    public void run() {
      Transaction tx = graphDb.beginTx();
      PrintWriter out = null;
      Random rand = new Random(1618 + clientId);
      try {
        // true for append
        out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)));

        // warmup
        int i = 0;
        int queryCount = queries.size();
        long warmupStart = System.nanoTime();
        while (System.nanoTime() - warmupStart < BenchConstants.WARMUP_TIME) {
          if (i % 10000 == 0) {
            tx.success();
            tx.close();
            tx = graphDb.beginTx();
          }
          PathQuery.run(graphDb, queries.get(rand.nextInt() % queryCount));
          ++i;
        }

        // measure
        i = 0;
        long paths = 0;
        long start = System.nanoTime();
        while (System.nanoTime() - start < BenchConstants.MEASURE_TIME) {
          if (i % 10000 == 0) {
            tx.success();
            tx.close();
            tx = graphDb.beginTx();
          }
          paths += PathQuery.run(graphDb, queries.get(rand.nextInt() % queryCount));
          ++i;
        }
        long end = System.nanoTime();
        double totalSeconds = (end - start) * 1. / 1e9;
        double queryThput = ((double) i) / totalSeconds;
        double pathThput = ((double) paths) / totalSeconds;

        // cooldown
        long cooldownStart = System.nanoTime();
        while (System.nanoTime() - cooldownStart < BenchConstants.COOLDOWN_TIME) {
          PathQuery.run(graphDb, queries.get(rand.nextInt() % queryCount));
          ++i;
        }
        out.printf("%.1f %.1f\n", queryThput, pathThput);

      } catch (Exception e) {
        System.err.printf("Client %d throughput bench exception: %s\n", clientId, e);
        System.exit(1);
      } finally {
        if (out != null) {
          out.close();
        }
        tx.success();
        tx.close();
      }
    }
  }

}
