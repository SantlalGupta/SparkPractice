package com.batch.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
 * This example demonstrate to use of spark.sql.functions, which are performing on group data.
 * here once grouping of data done, then we can take first, last, collect_list and count number of elements in that group.
 * output :
 * +------+-----------+----------+--------------------+-----------------------+
 * |custid|first_event|last_event|         user_events|Number_Of_Event_By_User|
 * +------+-----------+----------+--------------------+-----------------------+
 * |   101|      login|    logout|[login, search, s...|                      7|
 * |   103|     search|       pay|[search, select, ...|                      6|
 * |   102|     search|     goout|[search, select, ...|                      3|
 * +------+-----------+----------+--------------------+-----------------------+
 *
 */
object ConvertRowToColumn_performAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()

    val inputdf = spark.read.schema(schema).option("header", true).csv("SparkBatch/testData/input/RowToColumnData");

    sqlway(spark, inputdf)

    dataframeway(inputdf)
  }

  def sqlway(spark: SparkSession, input: DataFrame) = {

    input.createOrReplaceTempView("user")

    val user_ac = spark.sql("select " +
      "custid, " +
      "first(event)  first_event," +
      "last(event) as last_event," +
      "collect_list(event) user_events," +
      "count(event) Number_Of_Event_By_User from user" +
      " group by custid")

    user_ac.show

    user_ac.write.format("json").saveAsTable("user_activity_sql")
  }

  def dataframeway(inputdf: DataFrame): Unit = {
    val user_activity: DataFrame =
      inputdf.groupBy("custid").agg(
        first("event") as "first_event",
        last("event") as "last_event",
        collect_list("event") as "user_events",
        count("event") as "Number_Of_Event_By_User")

    user_activity.show()
    user_activity.write.format("json").saveAsTable("user_activity_df")
  }

  def schema: StructType = StructType(List(
    new StructField("custid", IntegerType, false),
    new StructField("event", StringType, false)
  ))
}
