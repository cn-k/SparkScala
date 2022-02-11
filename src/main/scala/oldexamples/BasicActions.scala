package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BasicActions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder()
    .appName("Word Count")
    .master("local[4]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val rdd = sc.parallelize(List(1, 1, 2, 9, 4, 5, 36))
  //rdd.collect().foreach(println)
  //println(rdd.count)
  //println(rdd.countByValue())
  rdd.take(3).foreach(print)
  println()
  rdd.top(3).foreach(print)
  println()
  rdd.takeOrdered(5).foreach(print)
  println()
  rdd.takeSample(false, 2).foreach(print)
  println()
  println(rdd.reduce((x, y) => x + y))
  println(rdd.fold(0)((x, y) => x + y))
}
