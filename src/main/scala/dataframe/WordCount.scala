package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount extends App {

  Logger.getLogger("word-count").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("word-count")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val hikayeDF = spark.read.textFile("inputs/omer_seyfettin_forsa_hikaye.txt").flatMap(r => r.split(" ")).toDF
  println(hikayeDF.count())
  //val kelimeler = hikayeDF.flatMap(x => x.split(" "))
}
