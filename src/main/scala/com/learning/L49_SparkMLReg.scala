package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._

object L49_SparkMLReg extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/regression.txt"

  val spark = SparkSession
    .builder
    .appName("SparkML")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val inputRows = spark.sparkContext.textFile(dsPath)
  val data = inputRows.map(x => {
    val splitted = x.split(",")
    (splitted(0).toDouble, Vectors.dense(splitted(1).toDouble))
  })

  val df = data.toDF("label", "features")

  val trainTest = df.randomSplit(Array(0.5, 0.5))
  val trainingDF = trainTest(0)
  val testDF = trainTest(1)

  val lir = new LinearRegression()
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setMaxIter(100)
    .setTol(1E-6)

  val model = lir.fit(trainingDF)

  val fullPredictions = model.transform(testDF).cache()

  val predictionAndLabel = fullPredictions
    .select("prediction", "label")
    .rdd
    .map(x => (x.getDouble(0), x.getDouble(1)))

  for (prd <- predictionAndLabel) {
    println(prd)
  }
  spark.stop()
}
