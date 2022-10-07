package com.batch.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
 * This is example to demonstrate how to se pivot on data set.
 * Here I have first group the data set and then apply pivot on the column of which rows value we want to become column for output
 * and then perform aggregation on group data(here we can use first or last function on group data but output will be same).
 *
 * Input:
 * custid,product,purchase_amt
 * 101,cosmatic,1212.98
 * 101,shop,112.12
 * 101,shampoo,232.34
 * 102,shop,232.12
 * 102,shampoo,232.32
 * 103,cosmatic,2342.23
 *
 * +------+--------+-------+------+
 * |custid|cosmatic|shampoo|  shop|
 * +------+--------+-------+------+
 * |   101| 1212.98| 232.34|112.12|
 * |   102|    null| 232.32|232.12|
 * |   103| 2342.23|   null|  null|
 * +------+--------+-------+------+
 */
object ConvertRowToColumn_pivot {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val inputdf = spark.read.schema(schema).option("header",true).csv("SparkBatch/testData/input/pivot")

    inputdf.groupBy("custid")
      .pivot("product") // name of column, of which row values you want to become column
      .agg(last("purchase_amt")).show

  }

  def schema : StructType = StructType(List(
    new StructField("custid",StringType,false),
    new StructField("product",StringType,false),
    new StructField("purchase_amt",DoubleType,false)
  ))
}
