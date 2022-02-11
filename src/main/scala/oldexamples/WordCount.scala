package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount extends App {

  Logger.getLogger("wordcount").setLevel(Level.ERROR)
  //Spark session came with spark 2
  val sparkSession = SparkSession.builder()
    .appName("Word Count")
    .master("local[4]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val input = sc.textFile("inputs/omer_seyfettin_forsa_hikaye.txt")

  val wordCount = input.flatMap(i => i.split(" "))
    .map(words => (words, 1)).reduceByKey((a, b) => a + b)
  //wordCount.foreach(println)
  wordCount.sortBy(_._2, false).foreach(println)

}
