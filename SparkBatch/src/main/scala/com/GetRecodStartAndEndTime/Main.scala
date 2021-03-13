package com.GetRecodStartAndEndTime

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, last, rank}

/**
 * input :
 * version 	id	starttime	status
 * v1		1	09:00		new
 * v2		1	09:02		pending
 * v1		2	09:05		new
 * v3		1	09:07		partially filled
 * v2		2	09:08		filled
 *
 * output:
 * version 	id	starttime	endtime
 * v3		1	09:00		09:07
 * v2		2	09:05		09:08
 */
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val df = spark.read.option("header","true").csv("SparkBatch\\testData\\input\\GetRecodStartAndEndTime")

    val df1 = df.withColumn("rank",rank().over(Window.partitionBy(col("id")).orderBy("starttime")))
    val df2:DataFrame = df1.groupBy("id").agg(
      last("version").as("version"),
      first("starttime").as("start"),
      last("starttime").as("last"),
      last("status").as("status"))
    df2.show
  }
}
