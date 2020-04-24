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

  res.foreach(x => println(s"${x._1}      ${x._2}"))
}
