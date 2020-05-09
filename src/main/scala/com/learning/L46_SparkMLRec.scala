package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode}
import org.apache.spark.ml.recommendation._

object L46_SparkMLRec extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/u.data"
  val idDescPath = "hdfs://localhost:9000/datasets/u.item"

  val spark = SparkSession
    .builder
    .appName("SparkML")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val moviesNames = spark.sparkContext.textFile(idDescPath).map(x => {
    val split = x.split("\\|")
    (split(0).trim.toInt, split(1).trim())
  }).toDF("movieId", "movieName")

  case class Rating(userId: Int, movieId: Int, rating: Float)


  val raw = spark.read.textFile(dsPath)
  val ratings = raw.map(x => {
    val splitted = x.split('\t')
    Rating(splitted(0).toInt, splitted(1).toInt, splitted(2).toFloat)
  }).toDF()

  println("Starting training model")
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")

  val model = als.fit(ratings)
  println("Done with training")

  val userId: Int = 0
  val users = Seq(userId).toDF("userId")

  val rec = model.recommendForUserSubset(users, 10 )

  val expanded = rec
    .select($"userId", explode($"recommendations").as("recommendations"))
    .select($"userId", $"recommendations".getItem("movieId").alias("recommendedMovie"))

  val expandedAndNamed = expanded
    .join(moviesNames, expanded("recommendedMovie") === moviesNames("movieId"), "inner")
    .select("userId", "recommendedMovie", "movieName")

  expandedAndNamed.show()
  spark.stop()
}
