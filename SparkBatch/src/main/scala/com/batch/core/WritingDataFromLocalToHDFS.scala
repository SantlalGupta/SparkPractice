package com.batch.core

import org.apache.spark.sql.SparkSession

object WritingDataFromLocalToHDFS {
  def main(args: Array[String]): Unit = {

    val sp = SparkSession.builder().master("local").appName("test").getOrCreate()

    val df = sp.read.csv("SparkBatch/testData/input/test")

    df.write.csv("hdfs://localhost:9000/output")
  }

}
