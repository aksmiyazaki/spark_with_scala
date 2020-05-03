package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L19_WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/book/book.txt"

  val sc = new SparkContext("local[*]", "RatingsCounter")

  val rdd = sc.textFile(dsPath)

  val res = rdd.flatMap(x => x.split(" ")).countByValue()

  res.foreach(println)
}
