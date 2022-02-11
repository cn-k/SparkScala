package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source

object BroadcastVariable extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[4]","Join")

  val broadcastProduct = sc.broadcast(loadProducts)

  val orderItemRDD = sc.textFile("inputs/order_items.csv").filter(!_.contains("orderItemName"))

  val orderItemPairRDD = orderItemRDD.map(makeOrderItemsRDD)
  val sortedOrders = orderItemPairRDD.reduceByKey((x,y) => x+y).sortBy(_._2, false).take(10)

  sortedOrders.map(s => (broadcastProduct.value(s._1),s._2)).foreach(println)


  def loadProducts():Map[Int,String]={
    val product = Source.fromFile("inputs/products.csv")
    val lines = product.getLines().filter(!_.contains("productId"))
    var productAndItem :Map[Int,String] = Map()
    for(line <- lines){
      val productId = line.split(",")(0).toInt
      val productName = line.split(",")(2)
      productAndItem += (productId -> productName)
    }
    productAndItem
  }

  def makeOrderItemsRDD(line:String)={
    val orderItemName = line.split(",")(0)
    val orderItemOrderId = line.split(",")(1)
    val orderItemProductId = line.split(",")(2).toInt
    val orderItemQuantiity = line.split(",")(3)
    val orderItemSubTotal = line.split(",")(4).toFloat
    val orderItemProductPrice = line.split(",")(5)
    (orderItemProductId, orderItemSubTotal)
  }



}
