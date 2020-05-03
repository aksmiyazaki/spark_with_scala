package com.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L21_SortingData extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/book/book.txt"

  val sc = new SparkContext("local[*]", "RatingsCounter")

  val rdd = sc.textFile(dsPath)

  val res = rdd.flatMap(x => x.split("\\W+"))
                .map(x => (x.toLowerCase, 1))
                .reduceByKey((x,y) => x + y)
                .map(x => (x._2, x._1))
                .sortByKey()
                .collect

  //res.foreach(x => println(s"${x._1}      ${x._2}"))


  // enhancing with stop word removal
  val sw = List("i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now")
  val resEnhanced = rdd.flatMap(x => x.split("\\W+"))
    .map(x => (x.trim.toLowerCase, 1))
    .filter(x => !sw.contains(x._1))
    .reduceByKey((x,y) => x + y)
    .map(x => (x._2, x._1))
    .sortByKey()
    .collect

  resEnhanced.foreach(x => println(s"${x._1}      ${x._2}"))

}
