package com.iflytek.scala.spark.batch

import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.catalog._
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types._
import org.apache.spark.sql.functions._
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.spark.sql.SparkSession

object IcebergDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("IcebergDemo")
      .getOrCreate()

    val name = TableIdentifier.of("default","iceberg_tab01");
    val df1=spark.range(1000).toDF.withColumn("level",lit("1"))
    val df1_schema = SparkSchemaUtil.convert(df1.schema)
    val partition_spec=PartitionSpec.builderFor(df1_schema).identity("level").build
    val tables = new HadoopTables(spark.sessionState.newHadoopConf())
    val table = tables.create(df1_schema, partition_spec, "tmp/iceberg_tab01")
    df1.write.format("iceberg").mode("append")
      .save("tmp/iceberg_tab01")
  }

}
