package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object InactiveUsers extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("DataFrameIntro")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = spark.sparkContext

  val ordersSchema = StructType(Array(
    StructField("order_id", IntegerType, true),
    StructField("order_date", TimestampType, true),
    StructField("order_customer_id", IntegerType, true),
    StructField("order_status", StringType, true))
  )

  val orders = spark.read
    .format("csv")
    .option("sep",",")
    .option("header","false")
    .option("inferSchema","false")
    .schema(ordersSchema)
    .load("/Users/cenkakdeniz/Desktop/retail_db-master/orders")
  orders.show(false)

  val customersSchema = StructType(Array(
    StructField("customer_id", IntegerType, true),
    StructField("customer_fname", StringType, true),
    StructField("customer_lname", StringType, true),
    StructField("x_col", StringType, true),
    StructField("y_col", StringType, true),
    StructField("address", StringType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("postal_code", IntegerType, true))
  )

  val customers = spark.read
    .format("csv")
    .option("sep",",")
    .option("header","false")
    .option("inferSchema","false")
    .schema(customersSchema)
    .load("/Users/cenkakdeniz/Desktop/retail_db-master/customers")
  customers.show()

  customers.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")
  val query =
    """
       select distinct c.customer_fname, c.customer_lname from orders o left join customers c
       on c.customer_id=o.order_customer_id where o.order_customer_id is not null
       order by c.customer_fname, c.customer_lname
      """
  spark.sql(query).show()


}
