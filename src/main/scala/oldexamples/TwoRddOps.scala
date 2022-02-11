package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object TwoRddOps extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder()
    .appName("Word Count")
    .master("local[4]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val firstRdd = sc.makeRDD(List(1, 2, 3, 4, 5))
  val secondRdd = sc.makeRDD(List(6, 7, 8, 9, 10))

  firstRdd.union(secondRdd).foreach(println)
  val thirdRdd = sc.makeRDD(List(2, 4, 6, 8))
  val forthRdd = sc.makeRDD(List(3, 6, 9))
  thirdRdd.intersection(forthRdd).foreach(println)
  thirdRdd.subtract(forthRdd).foreach(println)
}
