package edu.berkeley.cs.succinctgraph.neo4jbench

import java.util.{HashSet => JHashSet}

import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.JavaConverters._

object CheckGraph {

  def main(args: Array[String]) {
    val neo4jPath = args(0)
    val graphPath = args(1)
    println(s"neo4j path: $neo4jPath")
    println(s"edge.csv path: $graphPath")
    checkEdges(neo4jPath, graphPath)
  }

  def checkEdges(neo4jPath: String, graphPath: String) = {
    val edges = buildEdgesFromNeo4j(neo4jPath)
    println("building edges from neo4j: done")
    val graphEdges = new JHashSet[Long]()

    var numRelationships = 0
    var numBuilt = 0
    scala.io.Source.fromFile(graphPath).getLines().foreach { line =>
      val splits = line.split(",")
      val edge = pack(splits(0).toInt, splits(1).toInt)
      if (!edges.contains(edge)) {
        sys.error(s"neo4j does not contain edge $edge!")
      }
      if (!graphEdges.contains(edge)) {
        numRelationships += 1
        graphEdges.add(edge)
      }

      numBuilt += 1
      if (numBuilt % 100000 == 0) println(s"num built $numBuilt")
    }
    if (numRelationships != edges.size()) {
      println(s"edge.csv has $numRelationships unique edges, but neo4j has ${edges.size()}")
      println(s"Larger - Smaller: ${setDiff(edges, graphEdges)}")
    } else {
      println("OK: Edges are the same (ignoring node properties).")
    }
  }

  @inline
  def pack(v: Int, w: Int): Long = (v << 32) | w

  def setDiff(set1: JHashSet[Long], set2: JHashSet[Long]): String = {
    val setA = if (set1.size() < set2.size()) set1 else set2
    val setB = if (set1.size() < set2.size()) set2 else set1
    val sb = new StringBuilder()
    setB.iterator().asScala.foreach { case edge =>
      if (!setA.contains(edge)) sb.append(edge).append("; ")
    }
    sb.toString()
  }

  def buildEdgesFromNeo4j(neo4jPath: String): JHashSet[Long] = {
    val graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(neo4jPath)
    BenchUtils.registerShutdownHook(graphDb)

    val tx = graphDb.beginTx()
    try {
      val allRels = GlobalGraphOperations
        .at(graphDb)
        .getAllRelationships
        .asScala

      val edgeTable = new JHashSet[Long]()
      var numBuilt = 0
      for (relationship <- allRels) {
        val v = relationship.getStartNode.getId.toInt
        val w = relationship.getEndNode.getId.toInt
        edgeTable.add(pack(v, w))

        numBuilt += 1
        if (numBuilt % 100000 == 0) println(s"num built $numBuilt")
      }

      edgeTable
    } finally {
      tx.close()
    }
  }

}
