package rdd

import org.apache.spark.sql.SparkSession

object Avarage extends App {

  val sparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("filter app")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val calismaSaat = List(("Ahmet",8),("Burcu",7),("Yesim",7), ("Ahmet", 9), ("Yesim", 6), ("Yesim", 8), ("Burcu", 5))
  val calismaRDD = sc.parallelize(calismaSaat)
  val resRDD = calismaRDD.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1/x._2)

  resRDD.foreach(println)

}
