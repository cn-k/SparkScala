package oldexamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Join extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("filter app")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val order_items = sc.textFile("Inputs/order_items.csv").filter(x => !x.contains("orderItemName"))
  val products = sc.textFile("Inputs/products.csv").filter(x => !x.contains("productDescription"))

  val orderItemsPair = order_items.map(x => makeOrderItemPair(x))
  val productsPair = products.map(x => makeProductPair(x))
  val joinedRdd = order_items.map(x => (x.split(",")(2), x)).join(products.map(y => (y.split(",")(0), y)))
  println(joinedRdd.count())
  //val joinedRdd = orderItemsPair.join(productsPair)

  def makeOrderItemPair(line: String) = {
    val orderItemName = line.split((",") (0))
    val orderItemOrderId = line.split((",") (1))
    val orderItemProductId = line.split((",") (2))
    val orderItemQuantity = line.split((",") (3))
    val orderItemSubTotal = line.split((",") (4))
    val orderItemProductPrice = line.split((",") (5))
    (orderItemProductId, (orderItemName, orderItemOrderId, orderItemProductId, orderItemQuantity, orderItemSubTotal, orderItemProductPrice))
  }

  def makeProductPair(line: String) = {
    val productId = line.split((",") (0))
    val productCategoryId = line.split((",") (1))
    val productName = line.split((",") (2))
    val productDescription = line.split((",") (3))
    val productPrice = line.split((",") (4))
    val productImage = line.split((",") (5))
    (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
  }
}
