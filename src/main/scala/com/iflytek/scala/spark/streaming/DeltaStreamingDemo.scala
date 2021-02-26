package com.iflytek.scala.spark.streaming

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object DeltaStreamingDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("rateSource")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("rate")
      //每秒1000条
      .option("rowsPerSecond", 1000)
      .option("rampUpTime", 1)
      .option("numPartitions", 3)
      .load()

    df.withColumn("timestamp", to_date($"timestamp")).writeStream
      .format("delta")
      .partitionBy("timestamp")
      .option("checkpointLocation", "tmp/checkpoint")
      .option("path", "tmp/delta_tab02")

//      .format("console")
//      .outputMode("update")
//      .option("truncate",false)

      .start()
      .awaitTermination()

  }

}
