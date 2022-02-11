package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapFlatMap extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[4]").setAppName("MapFlatMap App")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("Inputs/simple_data.csv").filter(x => !x.contains("sirano"))
  rdd.foreach(println)
  rdd.map(x => x.toUpperCase()).take(10).foreach(println)
  rdd.flatMap(x => x.split(",")).map(y => y.toUpperCase()).take(10).foreach(println)

}
