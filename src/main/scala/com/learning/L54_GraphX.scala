package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession


object L54_GraphX extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val namesPath = "hdfs://localhost:9000/datasets/Marvel-names.txt"
  val graphPath = "hdfs://localhost:9000/datasets/Marvel-graph.txt"

  def parseNames(line: String): Option[(VertexId, String)] = {
    val splitted = line.split("\"")
    if(splitted.length > 1) {
      val heroId = splitted(0).trim().toLong
      if (heroId < 6487) {
        return Some((heroId, splitted(1).trim()))
      }
    }
    None
  }

  def parseGraph(line: String): List[Edge[Int]] = {
    val splitted = line.split("\\s")
    val origin = splitted(0)
    (for (x <- 1 to splitted.length - 1) yield Edge(origin.toLong, splitted(x).toLong, 0)).toList
  }

  val sc = SparkSession
    .builder
    .appName("SparkML")
    .master("local[*]")
    .getOrCreate()

  val vertex = sc.sparkContext.textFile(namesPath).flatMap(parseNames)
  val edges = sc.sparkContext.textFile(graphPath).flatMap(parseGraph)

  val default = "Nobody"
  val graph = Graph(vertex, edges, default).cache()

  println("Top 10 most-connected superheroes")
  graph.degrees.join(vertex).sortBy(_._2._1, ascending=false).take(10).foreach(println)

  println("Computing degrees of separation from Spiderman....")

  val root: VertexId = 5306

  val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

  val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(
    (id, attr, msg) => math.min(attr, msg),

    triplet => {
      if (triplet.srcAttr != Double.PositiveInfinity) {
        Iterator((triplet.dstId, triplet.srcAttr + 1))
      } else {
        Iterator.empty
      }
    },

    (a,b) => math.min(a,b)
  ).cache()

  bfs.vertices.join(vertex).take(100).foreach(println)

  println("Degrees from SpiderMan to Adam 3031")
  bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)
}
