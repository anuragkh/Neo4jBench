package edu.berkeley.cs.succinctgraph.neo4jbench.load;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.logging.Logger;

public class GraphLoader {

  private static GraphDatabaseService db = null;
  private static Index<Node> idIndex = null;
  private static int entryCount = 0;

  private static final Logger LOG = Logger.getLogger("GraphLoader");
  private static final int TX_COUNT = 1000;

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: loader [output] [input]");
      return;
    }

    String dbPath = args[0];
    String input = args[1];

    Properties p = new Properties();
    p.setProperty("db_path", dbPath);

    initialize(p);
    bulkLoad(input);
    db.shutdown();
  }

  public static void initialize(Properties p) throws Exception {
    LOG.info("Initializing db...");
    String dbPath = p.getProperty("db_path", "neo4j-data");
    LOG.info("Data path = " + dbPath);

    LOG.info("Initializing tuned database...");
    db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    LOG.info("Completed initializing tuned database.");

    LOG.info("Database initialization: " + db.toString());

    LOG.info("Initializing ID index...");
    try (Transaction tx = db.beginTx()) {
      idIndex = db.index().forNodes("identifier");
      tx.success();
    }
    LOG.info("Database initialization: " + idIndex.toString());
  }

  public static void add(int src, int edgeLabel, int dst) throws Exception {
    Node srcNode = db.createNode();
    srcNode.setProperty("id", src);
    idIndex.add(srcNode, "id", src);

    Node dstNode = db.createNode();
    dstNode.setProperty("id", dst);
    idIndex.add(dstNode, "id", dst);

    String edgeLabelStr = "p" + edgeLabel;
    RelationshipType relType = DynamicRelationshipType.withName(edgeLabelStr);
    Relationship rel = srcNode.createRelationshipTo(dstNode, relType);
    rel.setProperty("time", System.currentTimeMillis());

    entryCount++;
  }

  public static void bulkLoad(String file) {
    Transaction tx = db.beginTx();
    try (BufferedReader r = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = r.readLine()) != null) {
        String[] terms = line.split("\\s+");
        int src = Integer.valueOf(terms[0]);
        int edgeType = Integer.valueOf(terms[1]);
        int dst = Integer.valueOf(terms[2]);
        add(src, edgeType, dst);
        if (entryCount % TX_COUNT == 0) {
          System.out.println("Processed " + entryCount + " entries...");
          tx.success();
          tx = db.beginTx();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      tx.failure();
    }
  }
}
