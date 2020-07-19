package com.batch.core

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * Applying any cast function column and if column have NULL value than it will raise below exception:
  * Caused by: java.lang.NullPointerException: Value at index 0 is null
  * at org.apache.spark.sql.Row$class.getAnyValAs(Row.scala:472)
  * at org.apache.spark.sql.Row$class.getLong(Row.scala:231)
  * at org.apache.spark.sql.catalyst.expressions.GenericRow.getLong(rows.scala:166)
  *
  * In order to fix this we need NULL value to initital value like for Long it is 0
  */
object NVLFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
   val df =  spark.read.option("header", true).schema(schema).csv("SparkBatch/testData/input/NVLFunctionTest")

   df.show
import org.apache.spark.sql.functions._
    import spark.implicits._
   val df1 =  df.withColumn("amt",NVL($"amt",0))
    df1.show()
   println( df.agg(sum("amt").cast("long")).first().getLong(0))
//    println(df.agg(sum("amt").cast("long")).first().getLong(0))

  }

  def NVL(col:Column,value:Any) : Column = {
    when(col.isNull,lit(value)).otherwise(col)
  }

  def schema():StructType = StructType(List(StructField("id",IntegerType,false),StructField("amt",IntegerType,false)))
}
