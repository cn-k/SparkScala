package cca175

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Functions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("Functions")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  val employees = List((1, "Scott", "Tiger", 1000.0,
    "united states", "+1 123 456 7890", "123 45 6789"
  ),
    (2, "Henry", "Ford", 1250.0,
      "India", "+91 234 567 8901", "456 78 9123"
    ),
    (3, "Nick", "Junior", 750.0,
      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
    ),
    (4, "Bill", "Gomes", 1500.0,
      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
    )
  )
  import spark.implicits._
  val employeesDF = employees.toDF("employee_id", "first_name",
    "last_name", "salary",
    "nationality", "phone_number",
    "ssn"
  )
  //employeesDF.select(upper(col("nationality")))
  employeesDF.groupBy(upper(col("nationality"))).count().show()
  employeesDF.
    select(concat(col("first_name"), lit(", "), col("last_name")).alias("full_name")).
    show()

  employeesDF.
    select("employee_id", "nationality").
    withColumn("nationality_upper", upper(col("nationality"))).
    withColumn("nationality_lower", lower($"nationality")).
    withColumn("nationality_initcap", initcap(employeesDF("nationality"))).
    withColumn("nationality_length", length(col("nationality"))).
    show

  val l = List("x","y")
  val dummyDF = l.toDF("DUMMY")
  dummyDF.show()

  dummyDF.select(substring(lit("Hello World"), 7, 5)).
    show

  dummyDF.select(split(lit("Hello World, how are you"), " ")).
    show(false)

  dummyDF.select(split(lit("Hello World, how are you"), " ")(2)).
    show(false)

  dummyDF.select(lpad(lit("Hello"), 10, "-").alias("dummy")).show

  dummyDF.select(lpad(lit(2), 2, "0").alias("dummy")).show

  val lst = List("   Hello.    ")
  val df = lst.toDF("dummy")

  df.withColumn("ltrim", ltrim(col("dummy"))).
    withColumn("rtrim", rtrim(rtrim(col("dummy")), ".")).
    withColumn("trim", trim(trim(col("dummy")), ".")).
    show()

  val datetimes = List(("2014-02-28", "2014-02-28 10:00:00.123"),
    ("2016-02-29", "2016-02-29 08:08:08.999"),
    ("2017-10-31", "2017-12-31 11:59:59.123"),
    ("2019-11-30", "2019-08-31 00:00:00.000")
  )
  val datetimesDF = datetimes.toDF("date", "time")
  datetimesDF.show(false)

  datetimesDF.
    withColumn("date_add_date", date_add($"date", 10)).
    withColumn("date_add_time", date_add($"time", 10)).
    withColumn("date_sub_date", date_sub($"date", 10)).
    withColumn("date_sub_time", date_sub($"time", 10)).
    show(false)

  datetimesDF.
    withColumn("date_trunc", trunc($"date", "MM")).
    withColumn("time_trunc", trunc($"time", "yyyy")).
    show(false)

  datetimesDF.
    withColumn("date_dt", date_trunc("HOUR", $"date")).
    withColumn("time_dt", date_trunc("HOUR", $"time")).
    show(false)

  datetimesDF.
    withColumn("date_year", year($"date")).
    withColumn("time_year", year($"time")).
    show(false)

  datetimesDF.
    withColumn("date_month", month($"date")).
    withColumn("time_month", month($"time")).
    show(false)

  datetimesDF.
    withColumn("date_ym", date_format($"date", "yyyyMM")).
    withColumn("time_ym", date_format($"time", "yyyyMM")).
    show(false)

  datetimesDF.printSchema()

  datetimesDF.
    withColumn("date_dow", dayofweek($"date")).
    withColumn("time_dow", dayofweek($"time")).
    withColumn("date_dom", dayofmonth($"date")).
    withColumn("time_dom", dayofmonth($"time")).
    withColumn("date_doy", dayofyear($"date")).
    withColumn("time_doy", dayofyear($"time")).
    show(false)

  datetimesDF.
    withColumn("time_ts", date_format($"time", "yyyyMMddHHmmss")).
    show(false)

  datetimesDF.
    withColumn("date_us", date_format($"date", "MM-dd-yyyy")).
    withColumn("date_f", date_format($"date", "dd-MMM-yyyy")).
    show(false)
}
