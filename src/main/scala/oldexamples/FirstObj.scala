package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FirstObj extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //old spark context creation method
  val sparkConf = new SparkConf()
    .setAppName("Word Count")
    .setMaster("local[4]")
    .setExecutorEnv("spark.executor.memory", "4g")
  //Spark session came with spark 2
  val sparkSession = SparkSession.builder()
    .appName("Word Count")
    .master("local[4]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val lines = sc.textFile("Inputs/omer_seyfettin_forsa_hikaye.txt")
  //TOPLAM SATIR SAYISI
  println(lines.count())
  //KELİMELERE AYIRMA
  val words = lines.flatMap(l => l.split(" "))
  //HER KELİMEDEN KAÇ TANE OLDUĞUNU HESAPLAMA
  val wordsCount = words.map(w => (w, 1)).reduceByKey((x, y) => x + y)
  wordsCount.foreach(println)
  //TOPLAM KELİME SAYISINI EKRANA YAZDIRMA
  println(wordsCount.count)
  //EN ÇOK TEKRARLANAN KEİLMELERİ EKRANA YAZDIRMA
  wordsCount.map(x => (x._2, x._1)).sortByKey(false).take(20).foreach(println)
  println(words.distinct(3).count())
}
