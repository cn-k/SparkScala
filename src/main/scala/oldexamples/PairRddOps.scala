package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRddOps extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("filter app")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val rdd = sc.textFile("Inputs/simple_data.csv")
  val noHeaderRdd = rdd.filter(x => !x.contains("sirano"))
  val meslekMaas = noHeaderRdd.map(x => meslekMaasPair(x))
  //meslekMaas.foreach(println)
  val preparationRdd = meslekMaas.mapValues(x => (x, 1))
  //preparationRdd.foreach(println)
  val meslekMaasRBK = preparationRdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  //meslekMaasRBK.foreach(println)
  meslekMaasRBK.mapValues(x => x._1 / x._2).foreach(println)


  def meslekMaasPair(line: String) = {
    val meslek = line.split(",")(3)
    val maas = line.split(",")(5).toDouble
    (meslek, maas)
  }

}
