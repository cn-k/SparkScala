package cca175

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object OrderCustomerJoin extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("Employees")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  spark.read.schema("")
}
