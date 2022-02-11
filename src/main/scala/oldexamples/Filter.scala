package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Filter extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("filter app")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val rdd = sc.textFile("Inputs/OnlineRetail.csv")
  val noHeaderRdd = rdd.mapPartitionsWithIndex((indx, iter) => if (indx == 0) iter.drop(1) else iter)
  //description kısmında COFFEE geçenler ve birim fiyatı 20 den büyük olanları getir
  noHeaderRdd.filter(x =>
    x.split(";")(2)
      .contains("COFFEE") &&
      x.split(";")(5)
        .trim.replace(",", ".")
        .toFloat > 20.0F)
    .take(15).foreach(println)
  //satışı iptal olanlar
  val canceledSales = noHeaderRdd.filter(x => x.split(";")(0).startsWith("C"))
  val pairRdd = canceledSales.map(x => ("canceled", (x.split(";")(3).toDouble) * (x.split(";")(5).replace(",", ".").toDouble)))
  pairRdd.take(5).foreach(println)
  pairRdd.reduceByKey((x, y) => x + y).take(5).foreach(println)

}
