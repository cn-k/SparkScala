package com.kubernetes.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BasicExample {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Spark session came with spark 2
  val sparkSession=SparkSession.builder()
    .appName("Basic Example")
    .master("local[4]")
    .config("spark.executor.memory","4g")
    .config("spark.driver.memory","2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val rakam = sc.makeRDD(List(1,2,3,4,5,6))
  rakam.map(x => x*x).take(6).foreach(println)
  rakam.filter(x => x>4).foreach(println)
}
