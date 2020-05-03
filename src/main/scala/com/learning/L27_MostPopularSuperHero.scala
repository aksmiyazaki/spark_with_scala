package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L27_MostPopularSuperHero extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/Marvel-graph.txt"
  val idDescPath = "hdfs://localhost:9000/datasets/Marvel-names.txt"

  val sc = new SparkContext("local[*]", "RatingsCounter")
  val dataRdd = sc.textFile(dsPath)
  val namesRdd = sc.textFile(idDescPath)

  def procDataLineRdd(x: String): (Int, Int) = {
    val split = x.split("\\s+")
    (split(0).toInt, split.length - 1)
  }

  def procNamesRdd(x: String): Option[(Int, String)] = {
    val split = x.split("\"")
    if (split.length < 2) {
      None
    } else {
      Some((split(0).trim().toInt, split(1).trim()))
    }
  }

  val procNames = namesRdd.flatMap(procNamesRdd)

  val mostPopular = dataRdd.map(procDataLineRdd)
    .reduceByKey((x, y) => x + y)
    .map(x => (x._2, x._1))
    .max()

  val mostPopularName = procNames.lookup(mostPopular._2)(0)
  println(s"The most popular hero is ${mostPopular._2}:${mostPopularName} with ${mostPopular._1} popularity.")

  /// Solution with join
//  val procNames = namesRdd.flatMap(procNamesRdd)
//
//  val procData = dataRdd.map(procDataLineRdd)
//                        .reduceByKey((x, y) => x + y)
//
//  val namedData = procNames.join(procData)
//      .reduce((x, y) => if (x._2._2 > y._2._2) x else y)
//
//  println(s"The most popular hero is ${namedData._1}:${namedData._2._1} with ${namedData._2._2} popularity.")
}
