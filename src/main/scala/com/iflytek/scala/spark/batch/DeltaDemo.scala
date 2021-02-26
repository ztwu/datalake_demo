package com.iflytek.scala.spark.batch

import org.apache.spark.sql.{SaveMode, SparkSession}

object DeltaDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("delta")
      .getOrCreate()

    spark.range(0,5).toDF
      .write.format("delta")
      .mode(SaveMode.Overwrite)
      .save("tmp/delta_tab01")

    spark.range(5,10).toDF
      .write.format("delta")
      .mode(SaveMode.Append)
      .save("tmp/delta_tab01")

    val df = spark.read
      .format("delta")
      .load("tmp/delta_tab01")
    df.show()

    val df2 = spark.read
      .format("delta")
      .option("versionAsOf", 0)
      .load("tmp/delta_tab01")
    df2.show()

  }

}
