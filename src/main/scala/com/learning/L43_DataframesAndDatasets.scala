package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object L43_DataframesAndDatasets extends App {

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

  val peopleRawRdd = spark.sparkContext.textFile(dsPath).map(formatter)

  import spark.implicits._
  val people = peopleRawRdd.toDS.cache

  people.printSchema()
  people.createOrReplaceTempView("People")

  println("Printing people name")
  people.select("name").show()


  println("People younger than 21 yo")
  people.filter(x => x.age < 21).show()

  println("Count by Age")
  people.groupBy("age").count().show()

  println("Adds 10 years to people age.")
  people.select(people("name"), people("age"), people("age") + 10).show()

  spark.stop()


  spark.stop
}
