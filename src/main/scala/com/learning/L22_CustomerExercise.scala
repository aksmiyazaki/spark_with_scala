package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L22_CustomerExercise extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/customer-orders/customer-orders.csv"

  val sc = new SparkContext("local[*]", "RatingsCounter")

  val rdd = sc.textFile(dsPath)

  def procLine(line: String): (Int, Float) = {
    val sp = line.split(",")
    (sp(0).toInt, sp(2).toFloat)
  }

  val res = rdd.map(x => procLine(x))
               .reduceByKey((x, y) => x+y)
               .map(x => (x._2, x._1))
               .sortByKey()
               .collect()

  res.foreach(x => println(s"The user ${x._2} spent ${x._1}"))

}
