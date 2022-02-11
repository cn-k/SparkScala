package oldexamples

import org.apache.spark.{SparkConf, SparkContext}

object Test extends App {
  val conf = new SparkConf().setAppName("test-app").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val readmeData = sc.textFile("inputs/README.md")
  val wordCount = readmeData.flatMap(r => r.split(" +"))
  println(wordCount.count())
}
