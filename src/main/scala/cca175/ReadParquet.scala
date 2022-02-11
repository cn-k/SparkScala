package cca175

import org.apache.spark.sql.SparkSession
object ReadParquet extends App {
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("ReadOrders")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val ordersDF = spark.read
    .parquet("outputs/orders_parquet/part-00000-52fdafa8-0990-4994-81f0-29b4add74a96-c000.snappy.parquet")
  ordersDF.show()
}
