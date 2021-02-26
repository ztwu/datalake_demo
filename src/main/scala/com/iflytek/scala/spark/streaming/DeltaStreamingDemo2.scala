package com.iflytek.scala.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DeltaStreamingDemo2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("rateSource")
      .getOrCreate()

    val df = spark.readStream
      .format("delta")
      .option("path", "tmp/delta_tab02")
      .load()

    df.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate",false)

      .start()
      .awaitTermination()

  }

}
