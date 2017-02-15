package edu.berkeley.cs.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class BenchNeighbor {

  private static int WARMUP_N = 500000;
  private static int MEASURE_N = 500000;

  public static void main(String[] args) {
    String type = args[0];
    String db_path = args[1];
    String warmup_query_path = args[2];
    String query_path = args[3];
    String output_file = args[4];
    WARMUP_N = Integer.parseInt(args[5]);
    MEASURE_N = Integer.parseInt(args[6]);
    int numClients = Integer.parseInt(args[7]);
    boolean tuned = Boolean.valueOf(args[8]);

    String neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory.getDefaultValue();
    if (args.length >= 10) {
      neo4jPageCacheMemory = args[9];
    }

    List<Long> warmupQueries = new ArrayList<>();
    List<Long> queries = new ArrayList<>();
    BenchUtils.getNeighborQueries(warmup_query_path, warmupQueries);
    BenchUtils.getNeighborQueries(query_path, queries);

    if (type.equals("neighbor-latency")) {
      benchNeighborLatency(tuned, db_path, neo4jPageCacheMemory, warmupQueries, queries,
        output_file);
    } else if (type.equals("neighbor-throughput")) {
      benchNeighborThroughput(tuned, db_path, neo4jPageCacheMemory, warmupQueries, queries,
        numClients);
    }
  }

  private static void benchNeighborLatency(boolean tuned, String dbPath, String neo4jPageCacheMem,
    List<Long> warmupQueries, List<Long> queries, String output_file) {

    System.out.println("Benchmarking getNeighbor queries");
    GraphDatabaseService graphDb;
    if (tuned) {
      graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
        .setConfig(GraphDatabaseSettings.cache_type, "none")
        .setConfig(GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem).newGraphDatabase();
    } else {
      graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    }

    BenchUtils.registerShutdownHook(graphDb);
    Transaction tx = graphDb.beginTx();
    try {
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
      PrintWriter resOut = null;
      if (System.getenv("BENCH_PRINT_RESULTS") != null) {
        resOut = new PrintWriter(new BufferedWriter(new FileWriter(output_file + ".neo4j_result")));
      }

      if (tuned) {
        BenchUtils.fullWarmup(graphDb);
      }

      System.out.println("Warming up for " + WARMUP_N + " queries");
      for (int i = 0; i < WARMUP_N; i++) {
        if (i % 10000 == 0) {
          tx.success();
          tx.close();
          tx = graphDb.beginTx();
        }
        List<Long> neighbors = getNeighbors(graphDb, BenchUtils.modGet(warmupQueries, i));
      }

      System.out.println("Measuring for " + MEASURE_N + " queries");
      // measure
      for (int i = 0; i < MEASURE_N; i++) {
        if (i % 10000 == 0) {
          tx.success();
          tx.close();
          tx = graphDb.beginTx();
        }
        long queryStart = System.nanoTime();
        List<Long> neighbors = getNeighbors(graphDb, BenchUtils.modGet(queries, i));
        long queryEnd = System.nanoTime();
        double microsecs = (queryEnd - queryStart) / ((double) 1000);
        out.println(neighbors.size() + "," + microsecs);

        if (resOut != null) {
          // correctness validation
          Collections.sort(neighbors);
          String header = "node id: " + BenchUtils.modGet(queries, i);
          BenchUtils.print(header, neighbors, resOut);
        }
      }

      tx.success();
      out.close();
      if (resOut != null) {
        resOut.flush();
        resOut.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      BenchUtils.printMemoryFootprint();
      System.out.println("Shutting down database ...");
      graphDb.shutdown();
    }
  }

  private static void benchNeighborThroughput(boolean tuned, String dbPath,
    String neo4jPageCacheMem, List<Long> warmupQueries, List<Long> queries, int numClients) {

    GraphDatabaseService graphDb;
    System.out.println("About to open database");
    if (tuned) {
      graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
        .setConfig(GraphDatabaseSettings.cache_type, "none")
        .setConfig(GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem).newGraphDatabase();
    } else {
      graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    }
    System.out.println("Done opening");

    BenchUtils.registerShutdownHook(graphDb);
    Transaction tx = null;
    try {
      tx = graphDb.beginTx();
      if (tuned) {
        BenchUtils.fullWarmup(graphDb);
      }
    } finally {
      if (tx != null) {
        tx.success();
        tx.close();
      }
    }

    try {
      List<Thread> clients = new ArrayList<>(numClients);
      for (int i = 0; i < numClients; ++i) {
        clients.add(new Thread(new RunNeighborThroughput(i, warmupQueries, queries, graphDb)));
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

  public static List<Long> getNeighbors(GraphDatabaseService graphDb, long id) {
    List<Long> neighbors = new ArrayList<>();
    Node n = graphDb.getNodeById(id);
    Iterable<Relationship> rels = n.getRelationships(Direction.OUTGOING);
    for (Relationship r : rels) {
      neighbors.add(r.getOtherNode(n).getId());
    }
    return neighbors;
  }


  static class RunNeighborThroughput implements Runnable {
    private int clientId;
    private List<Long> warmupQueries, queries;
    private GraphDatabaseService graphDb;

    public RunNeighborThroughput(int clientId, List<Long> warmupQueries, List<Long> queries,
      GraphDatabaseService graphDb) {

      this.clientId = clientId;
      this.warmupQueries = warmupQueries;
      this.queries = queries;
      this.graphDb = graphDb;
    }

    public void run() {
      Transaction tx = graphDb.beginTx();
      PrintWriter out = null;
      Random rand = new Random(1618 + clientId);
      try {
        // true for append
        out = new PrintWriter(
          new BufferedWriter(new FileWriter("neo4j_throughput_get_nhbrs.txt", true)));

        // warmup
        int i = 0, queryIdx = 0;
        long warmupStart = System.nanoTime();
        while (System.nanoTime() - warmupStart < BenchConstants.WARMUP_TIME) {
          if (i % 10000 == 0) {
            tx.success();
            tx.close();
            tx = graphDb.beginTx();
          }
          queryIdx = rand.nextInt(warmupQueries.size());
          getNeighbors(graphDb, BenchUtils.modGet(warmupQueries, queryIdx));
          ++i;
        }

        // measure
        i = 0;
        long edges = 0;
        int querySize = queries.size();
        List<Long> neighbors;
        long start = System.nanoTime();
        while (System.nanoTime() - start < BenchConstants.MEASURE_TIME) {
          if (i % 10000 == 0) {
            tx.success();
            tx.close();
            tx = graphDb.beginTx();
          }
          queryIdx = rand.nextInt(querySize);
          neighbors = getNeighbors(graphDb, BenchUtils.modGet(queries, queryIdx));
          edges += neighbors.size();
          ++i;
        }
        long end = System.nanoTime();
        double totalSeconds = (end - start) * 1. / 1e9;
        double queryThput = ((double) i) / totalSeconds;
        double edgesThput = ((double) edges) / totalSeconds;

        // cooldown
        long cooldownStart = System.nanoTime();
        while (System.nanoTime() - cooldownStart < BenchConstants.COOLDOWN_TIME) {
          getNeighbors(graphDb, BenchUtils.modGet(warmupQueries, i));
          ++i;
        }
        out.printf("%.1f %.1f\n", queryThput, edgesThput);

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

  //    public static List<Long> getNeighbors(GraphDatabaseService graphDb,
  //                                                long id) {
  //        Node n = graphDb.getNodeById(id);
  //        Iterable<Relationship> rels = n.getRelationships(Direction.OUTGOING);
  //
  //        List<TimestampedId> timestampedNhbrs = new ArrayList<TimestampedId>();
  //        long timestamp, nhbrId;
  //        for (Relationship r : rels) {
  //            timestamp = (long) (r.getProperty("timestamp"));
  //            nhbrId = r.getOtherNode(n).getId();
  //            timestampedNhbrs.add(new TimestampedId(timestamp, nhbrId));
  //        }
  //
  //        List<Long> neighbors = new ArrayList<Long>(timestampedNhbrs.size());
  //        Collections.sort(timestampedNhbrs);
  //        for (TimestampedId timestampedId : timestampedNhbrs) {
  //            neighbors.add(timestampedId.id);
  //        }
  //        return neighbors;
  //    }

}
