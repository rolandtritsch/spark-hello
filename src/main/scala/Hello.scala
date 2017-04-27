package org.tritsch.spark.hello

import org.apache.spark.sql._

object Hello {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Hello World")
    .getOrCreate()

  def buildWordSizeHistogram(s: String): Dataset[(Int, Int)] = {
    import spark.implicits._
    val words = spark.sparkContext.parallelize(s.split(" "))
    words.map(_.size).groupBy(wordSize => wordSize).map{case (k, v) => (k, v.size)}.toDS()
  }

  def main(args: Array[String]): Unit = {
    require(args.size == 1, "Usage: Hello <string>")

    buildWordSizeHistogram(args(0)).show()

    spark.close()
  }
}