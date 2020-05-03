package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L25_PopularMovie extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/movies/u.data"

  val sc = new SparkContext("local[*]", "RatingsCounter")
  val rdd = sc.textFile(dsPath)

  def procRddLine(line: String):(Int, Int) = {
    val spl = line.split("\t")
    (spl(1).toInt, 1)
  }


  val res = rdd.map(s => procRddLine(s))
    .reduceByKey((x,y) => x + y)
    .map(x => (x._2,x._1))
    .sortByKey()
    .collect()

  res.foreach(x => println(s"Movie ${x._2} has ${x._1} popularity."))

}
