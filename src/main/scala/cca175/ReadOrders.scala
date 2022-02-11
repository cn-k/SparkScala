package cca175

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReadOrders extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("ReadOrders")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val ordersDF = spark.read
    .option("inferSchema","false")
    .schema("""order_id INT, order_date TIMESTAMP,
           order_customer_id INT, order_status STRING
     """)
    .option("sep",",")
    .format("csv")
    .load("/Users/cenkakdeniz/Desktop/retail_db-master/orders").as("orders")

  val customersDF = spark.read
    .option("inferSchema","false")
    .schema("""customer_id INT, name STRING, lname STRING,
           no1 STRING, no2 STRING , address STRING, city STRING, state STRING, postal_code INT
     """)
    .option("sep",",")
    .format("csv")
    .load("/Users/cenkakdeniz/Desktop/retail_db-master/customers").as("customers")
  customersDF.show(truncate = false)
  customersDF.selectExpr("customer_id","concat(name, ' ',  lname) as full_name").orderBy("full_name").sample(0.1).coalesce(1).write.parquet("outputs/orders_parquet")
  /*
  val joinedDF = customersDF.join(ordersDF, col("customers.customer_id") === col("orders.order_customer_id"), "leftouter")
  val customersWithoutOrder = joinedDF.filter("order_id is null")
  customersWithoutOrder.select(col("name"), col("lname")).orderBy(col("name").asc, col("lname").asc).show(100)


   */
  //joinedDF.show(10000, truncate=false)
}

