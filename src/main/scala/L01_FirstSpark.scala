import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object L01_FirstSpark extends App {
  val dsPath = "hdfs://localhost:9000/datasets/ml-100k/u.data"
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "RatingsCounter")

  val lines = sc.textFile(dsPath)

  val ratings = lines.map(x => x.toString().split("\t")(2))

  val results = ratings.countByValue()

  val sortedResults = results.toSeq.sortBy(_._1)

  sortedResults.foreach(println)
}
