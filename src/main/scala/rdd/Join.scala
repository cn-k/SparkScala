package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Join extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[4]","Join")
  val orderItemsRDD = sc.textFile("inputs/order_items.csv")

  val productsRDD = sc.textFile("inputs/products.csv")

  val orderItemPairRDD: RDD[(String, (String, String, String, String, String))] = orderItemsRDD.map(makeOrderItemsRDD)
  val productPairRDD = productsRDD.map(makeProductRDD)

  val orderItemsProductJoinedRDD = orderItemPairRDD.join(productPairRDD)
  def makeOrderItemsRDD(line:String)={
    val orderItemName = line.split(",")(0)
    val orderItemOrderId = line.split(",")(1)
    val orderItemProductId = line.split(",")(2)
    val orderItemQuantiity = line.split(",")(3)
    val orderItemSubTotal = line.split(",")(4)
    val orderItemProductPrice = line.split(",")(5)
    (orderItemProductId,(orderItemName,orderItemOrderId,orderItemQuantiity,orderItemSubTotal,orderItemProductPrice))
  }
  def makeProductRDD(line:String)={
    val productId = line.split(",")(0)
    val productCategoryId = line.split(",")(1)
    val productName = line.split(",")(2)
    val productDescription = line.split(",")(3)
    val productPrice = line.split(",")(4)
    val productImage  = line.split(",")(5)
    (productId, (productCategoryId,productName,productDescription,productPrice,productImage))
  }
  println("orderItemsRDD satır sayısı: " + orderItemsRDD.count())
  println("productsRDD satır sayısı: " + productsRDD.count())
  println("orderItemProductJoinedRDD satır sayısı: " + orderItemsProductJoinedRDD.count())
}

