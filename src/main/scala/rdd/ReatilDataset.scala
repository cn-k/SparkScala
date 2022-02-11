package rdd

import org.apache.spark.sql.SparkSession

object ReatilDataset extends App{
  val sparkSession=SparkSession.builder()
    .appName("Word Count")
    .master("local[4]")
    .config("spark.executor.memory","4g")
    .config("spark.driver.memory","2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext

  val retailRDDWithHeader = sc.textFile("inputs/OnlineRetail.csv")
  val retailRDD = retailRDDWithHeader.filter(!_.contains("Invoice"))
  println(retailRDD.first())
  retailRDD.filter(r => r.split(";")(5).trim.replace(",",".").toFloat > 20.0 &&  r.split(";")(2).contains("COFFEE")).take(5).foreach(println)

}
