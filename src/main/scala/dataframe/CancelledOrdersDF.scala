package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CancelledOrdersDF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("DataFrameIntro")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = spark.sparkContext

  val onlineRetailDF =  spark.read
    .format("csv")
    .option("sep",";")
    .option("header","true")
    .option("inferSchema","true")
    .load("inputs/OnlineRetail.csv")
  //val res = onlineRetailDF.map(r => (r.get(0).asInstanceOf[String], r.get(3).asInstanceOf[Int] ,  r.get(5).asInstanceOf[String].replace(",",".").toDouble)).toDF("InvoiceNo","Quantity", "UnitPrice")
  //res.printSchema()
  //res.show()
  //res.filter(col("InvoiceNo").startsWith("C"))

  val updatedDf = onlineRetailDF.withColumn("UnitPrice", regexp_replace(col("UnitPrice"), ",", ".").cast("Double"))
    .filter(col("InvoiceNo").startsWith("C"))
  updatedDf.printSchema()
  val sum = updatedDf.rdd.map(rdd => rdd(3).asInstanceOf[Int] * rdd(5).asInstanceOf[Double]).reduce(_+_)
  println(sum)
  //val result = onlineRetailDF.withColumn("UnitPrice", res.col("price"))
  //result.show()
  //updatedDf.show()
  //onlineRetailDF.withColumn("price", col.getField("value")).show()



}
