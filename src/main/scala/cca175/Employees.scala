package cca175

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit}

object Employees extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("Employees")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  import spark.implicits._
  val employees = List((1, "Scott", "Tiger", 1000.0, "united states"),
    (2, "Henry", "Ford", 1250.0, "India"),
    (3, "Nick", "Junior", 750.0, "united KINGDOM"),
    (4, "Bill", "Gomes", 1500.0, "AUSTRALIA")
  )
  val employeesDF = employees.
    toDF("employee_id",
      "first_name",
      "last_name",
      "salary",
      "nationality"
    )
  employeesDF.printSchema()
  employeesDF.show()

  employeesDF.select(col("employee_id"), concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    col("salary"),
    col("nationality")
  ).show()
}
