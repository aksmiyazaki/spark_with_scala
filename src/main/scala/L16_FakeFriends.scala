import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L16_FakeFriends extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val dsPath = "hdfs://localhost:9000/datasets/fakefriends/fakefriends.csv"

  val sc = new SparkContext("local[*]", "RatingsCounter")

  val lines = sc.textFile(dsPath)

  val filteredLines = lines.map(x => {
      val split = x.toString.split(',')
      (split(1), split(3).toInt) // This line gets the avg number of friends by first name
      //(split(2).toInt, split(3).toInt) // This line gets the avg number of friends by age
  })

  val res = filteredLines.mapValues(x => (x, 1))
                         .reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
                         .mapValues(x => x._1 / x._2)
                         .sortByKey(true).collect

  res.foreach(x => println(x.toString()))
}
