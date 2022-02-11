package oldexamples

import org.apache.spark.sql.{DataFrame, SparkSession}

object OrdersData extends App {

  ///Users/cenkakdeniz/Desktop/retail_db-master/orders
  val spark = SparkSession.builder()
    .appName("orders-data-read")
    .master("local[1]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  //val sc = sparkSession.sparkContext

  val orders: DataFrame = spark.read
    .option("sep", ",")
    .schema(
      """orderId INT, order_date TIMESTAMP, order_customer_id INT, order_status STRING
        |""".stripMargin)
    .csv("file:///Users/cenkakdeniz/Desktop/retail_db-master/orders/part-00000")
  orders.show()

}
