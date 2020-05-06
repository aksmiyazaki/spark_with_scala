package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object L44_DataframesAndDatasets extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/u.data"
  val idDescPath = "hdfs://localhost:9000/datasets/u.item"

  val spark = SparkSession
    .builder
    .appName("DSAndDf")
    .master("local[*]")
    .getOrCreate()

  case class Movie(movieId: Int)
  case class CountedMovie(movieId: Int, Count: Long)
  case class NamedMovie(movieId: Int, movieName: String)
  case class FinalMovie(movieId: Int, movieName: String, NumberOfRatings: Long)

  val moviesRdd = spark.sparkContext.textFile(dsPath).map(x => Movie(x.split("\t")(1).trim.toInt))
  val moviesNames = spark.sparkContext.textFile(idDescPath).map(x => {
    val split = x.split("\\|")
    NamedMovie(split(0).trim.toInt, split(1).trim())
  })

  import spark.implicits._
  val movieDs = moviesRdd.toDS
  val moviesNamesDs = moviesNames.toDS

  val topMovies = movieDs
    .groupBy("movieId")
    .count()
    .map(x => CountedMovie(x.getAs("movieId"), x.getAs("count")))


  val joined = topMovies
    .join(moviesNamesDs, topMovies("movieId") === moviesNamesDs("movieId"), "inner")
      .map(x => FinalMovie(x.getAs[Int]("movieId"), x.getAs[String]("movieName"), x.getAs[Long]("Count")))
      .orderBy(desc("NumberOfRatings"))

  joined.show()

  spark.stop()
}
