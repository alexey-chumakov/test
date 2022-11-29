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

}
