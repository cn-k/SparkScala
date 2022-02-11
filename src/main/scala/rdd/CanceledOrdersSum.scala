package rdd

import org.apache.spark.sql.SparkSession

object CanceledOrdersSum extends App {

  val sparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("filter app")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext

  val retailRDDWithHeader = sc.textFile("inputs/OnlineRetail.csv")
  val reatailRDD = retailRDDWithHeader.filter(!_.contains("Invoice"))
  val pairRetailRDD = reatailRDD.map(r => (r.split(";")(0),r.split(";")(3).toDouble * r.split(";")(5).replace(",",".").toDouble))
  val cancelledOrders = pairRetailRDD.filter(r => r._1.startsWith("C"))
  cancelledOrders.take(10).foreach(println)
  val res = cancelledOrders.map(_._2)
    .reduce((x,y) => x+y)
  println(res)
}
