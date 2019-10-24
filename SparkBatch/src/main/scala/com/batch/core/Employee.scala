package com.batch.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType}

object Employee {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Employee").getOrCreate()

    val read = spark.read.option("header",true)/*.format("yyyy-MM-dd")*/.schema(schema).csv("SparkBatch/testData/input/Employee")

  /* converting address field from String to Array[String]
  val arra = read.map(row=> {
      val addressStr = row.getAs("address").toString
      val add = addressStr.substring(2, addressStr.length - 3).split(",")
      Row(row.getAs("name").toString, add, row.getDate(2))
    })sql.Encoders.*/
    read.show()
  }

  def schema:StructType = StructType(List(
    new StructField("name",StringType,false),
    new StructField("address",StringType,false),
    new StructField("doj",DateType,false)
  ))

  def arraySchema:StructType = StructType(List(
    new StructField("name",StringType,false),
    new StructField("address",ArrayType(StringType),false),
    new StructField("doj",DateType,false)
  ))
}
