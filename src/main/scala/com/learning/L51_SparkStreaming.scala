package com.learning

import com.learning.L49_SparkMLReg.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

import scala.io.Source

object L51_SparkStreaming extends App{
  Logger.getRootLogger().setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val keyPath = "/Users/alexandre.miyazaki/Documents/git-personal/spark_with_scala/datasets/SparkScala3/twitter.txt"

  def setupTwitter(): Unit = {
    val lines = Source.fromFile(keyPath).getLines()
    for (line <- lines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        println(s"Setting twitter4j.oauth.${fields(0)} to ${fields(1)}")
        System.setProperty("twitter4j.oauth."+fields(0), fields(1))
      }
    }
  }

  setupTwitter()

  val ssc = new StreamingContext("local[*]", "PopHashTags", Seconds(10))

  val tweets = TwitterUtils.createStream(ssc, None)

  val statuses = tweets.map(status => status.getText())

  val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))

  val hashTags = tweetWords.filter(word => word.startsWith("#"))

  val hashTagKeyValues = hashTags.map(hs => (hs, 1))

  val hashTagCounts = hashTagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x, y) => x - y, Seconds(300), Seconds(10))

  val sortedResults = hashTagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

  sortedResults.print

  ssc.checkpoint("/tmp/checkpoints")
  ssc.start()
  ssc.awaitTermination()

}
