package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L26_PopularMovieWithTitle extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/movies/u.data"
  val idDescPath = "hdfs://localhost:9000/datasets/movies/u.item"

  val sc = new SparkContext("local[*]", "RatingsCounter")
  val popularityRdd = sc.textFile(dsPath)
  val idNameMappingRdd = sc.textFile(idDescPath)

  def procPopularityRddLine(line: String):(Int, Int) = {
    val spl = line.split("\t")
    (spl(1).toInt, 1)
  }

  def procIdMappingLine(line: String):(Int, String) = {
    val spl = line.split("\\|")
    (spl(0).toInt, spl(1))
  }

  val popularity = popularityRdd.map(s => procPopularityRddLine(s))
    .reduceByKey((x,y) => x + y)

  val idNameMapping = idNameMappingRdd.map(s => procIdMappingLine(s))

  val res = popularity.join(idNameMapping)
    .map(x => (x._2._1,(x._1, x._2._2)))
    .sortByKey()

  //res.foreach(println)
  res.foreach(x => println(s"The movie ${x._2._1}:${x._2._2} has ${x._1} popularity"))

}
