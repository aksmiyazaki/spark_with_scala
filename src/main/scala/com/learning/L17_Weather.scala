package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.{max, min}

object L17_Weather extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/1800/1800.csv"

  val sc = new SparkContext("local[*]", "RatingsCounter")

  val rdd = sc.textFile(dsPath)

  val res = rdd.map(x => {
                      val split = x.split(',')
                      (split(0), split(2), split(3).toFloat * 0.1f)
                    }).filter(x => x._2 == "TMIN")
                    .map(x => (x._1, x._3))
                    .reduceByKey((x, y) => min(x, y))
                    .collect
                    .sorted

  res.foreach(x => println(f"Station ${x._1} min temperature: ${x._2}%.2f C or ${x._2 * (9.0f/5.0f) + 32.0f}%.2f F" ))



  val resMax = rdd.map(x => {
    val split = x.split(',')
    (split(0), split(2), split(3).toFloat * 0.1f)
  }).filter(x => x._2 == "TMAX")
    .map(x => (x._1, x._3))
    .reduceByKey((x, y) => max(x, y))
    .collect
    .sorted

  resMax.foreach(x => println(f"Station ${x._1} max temperature: ${x._2}%.2f C or ${x._2 * (9.0f/5.0f) + 32.0f}%.2f F" ))

}
