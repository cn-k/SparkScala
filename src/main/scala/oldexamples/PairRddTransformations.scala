package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRddTransformations extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Spark session came with spark 2
  val sparkSession = SparkSession.builder()
    .appName("Pair RDD Transformations")
    .master("local[4]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext

  val yaslar = List(("Ahmet", 35), ("Burcu", 27), ("Yesim", 25))
  val rdd = sc.parallelize(yaslar)
  val underThirty = rdd.filter { case (key, value) => value < 30 }
  underThirty.take(3).foreach(println)

  //reduceByKey
  val rdd2 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
  rdd2.reduceByKey((x, y) => x + y).foreach(println)
  //groupByKey
  rdd.groupByKey().foreach(println)
}
