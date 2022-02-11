package cca175

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Transformations extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("Transformations")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val airlines_path = "inputs/flightmonth=200801"
  val airlines_all = spark.read.parquet(airlines_path)
  airlines_all.printSchema()
  airlines_all.show
  val airlines = airlines_all.
    select("Year", "Month", "DayOfMonth",
      "DepDelay", "ArrDelay", "UniqueCarrier",
      "FlightNum", "IsArrDelayed", "IsDepDelayed"
    )

  airlines.show

  airlines.
    filter("IsDepDelayed = 'YES' AND IsArrDelayed = 'NO'").
    count

  airlines.
    withColumn("FlightDate",
      concat(col("Year"),
        lpad(col("Month"), 2, "0"),
        lpad(col("DayOfMOnth"), 2, "0")
      )
    ).
    show

  import spark.implicits._

  val l = List("x")
  val dummyDF = l.toDF("DUMMY")

  dummyDF.select(current_date, date_format(current_date, "EEEE")).show


}
