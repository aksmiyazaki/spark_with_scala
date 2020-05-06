package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object L42_DataframesAndDatasets extends App {

  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

  def formatter(row: String): Person = {
    val splitted = row.split(',')
    Person(splitted(0).trim.toInt, splitted(1).trim, splitted(2).trim.toInt, splitted(3).trim.toInt)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/fakefriends.csv"
  val spark = SparkSession
    .builder
    .appName("DSAndDf")
    .master("local[*]")
    .getOrCreate()

  val people = spark.sparkContext.textFile(dsPath).map(formatter)

  import spark.implicits._
  val schemaPeople = people.toDS

  schemaPeople.printSchema()
  schemaPeople.createOrReplaceTempView("People")

  val teens = spark.sql("SELECT * FROM People WHERE age >= 13 AND age <= 18")
  teens.collect.foreach(println)

  spark.stop
}
