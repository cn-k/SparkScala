package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object PairRDD extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[4]","PairRDD-Ops")
  val insanlarRDD = sc.textFile("inputs/simple_data.csv").filter(!_.contains("sirano"))
  insanlarRDD.take(10)

  val meslekMaasPairRDD: RDD[(String, (Double, Double))] = insanlarRDD.map(meslekMaasPair).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  meslekMaasPairRDD.map(mm => (mm._1, mm._2._1/mm._2._2)).foreach(println)

  def meslekMaasPair(line:String): (String, (Double, Double)) = {
    val meslek = line.split(",")(3)
    val maas = line.split(",")(5)
    (meslek,(maas.toDouble,1))
  }
}
