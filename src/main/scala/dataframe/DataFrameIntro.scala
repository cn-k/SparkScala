package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameIntro extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("DataFrameIntro")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val dfFromList = sc.parallelize(List(1,2,3,4,5,6,4,5)).toDF("rakamlar")
  dfFromList.printSchema()

  val dfFromFileWithLoad = spark.read
    .format("csv")
    .option("sep",";")
    .option("header","true")
    .option("inferSchema","true")
    .load("inputs/OnlineRetail.csv")
  //dfFromFileWithLoad.printSchema()
 //same as above command
  val dfFromFileWithCsv = spark.read
    .option("sep",";")
    .option("header","true")
    .option("inferSchema","true")
    .csv("inputs/OnlineRetail.csv")
  //dfFromFileWithCsv.printSchema()
  println("\n Online Retail satır sayısı: " + dfFromFileWithLoad.count())

  //dfFromFileWithLoad.select("InvoiceNo", "Quantity").show(10)
  //below 3 command are same
  dfFromFileWithLoad.sort('Quantity).show(10)
  //dfFromFileWithLoad.sort($"Quantity").show(10)
  //dfFromFileWithLoad.sort(dfFromFileWithLoad.col("Quantity")).show(10)
  spark.conf.set("spark.sql.shuffle.partitiona","5")
  dfFromFileWithLoad.sort($"Quantity").explain()
}
