package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

trait AppBase extends App {

  protected val sparkConf: SparkConf = new SparkConf()
    .setAppName("Test App")
    .setMaster("local[4]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  protected val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate

  protected def createDataFrame(data: Seq[String]): DataFrame = {
    import spark.implicits._
    val dataFrame = spark.read.json(data.toDS)
    dataFrame
  }

  protected def distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    Math.sqrt(Math.pow(lat2 - lat1, 2) + Math.pow(lon2 - lon1, 2))
  }

}
