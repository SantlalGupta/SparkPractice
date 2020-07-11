package com.batch.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/*
 * Iterative technique of broadcast
 */
object Interative_broadcast {
  val custSchema: StructType = StructType(List(new StructField("id", IntegerType, false),
    new StructField("cid", StringType, false)
    , new StructField("name", StringType, false)))
  val loanSchema: StructType = StructType(List(new StructField("id", IntegerType, false),
    new StructField("cid", StringType, false)
    , new StructField("lid", StringType, false)
    , new StructField("type", StringType, false)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val cust = spark.read.option("header", "true").schema(custSchema)
      .csv("SparkBatch/testData/input/interactive_broadcast/Customer.txt").withColumn("pass", col("id") % 3).cache()

    val loan = spark.read.option("header", "true").schema(loanSchema)
      .csv("SparkBatch/testData/input/interactive_broadcast/Loan.txt").cache()
    loan.createOrReplaceTempView("loan")


    cust.show()
    loan.show

    var jdf0: DataFrame = null
    var jdf1: DataFrame = null
    var jdf2: DataFrame = null

    cust.filter(col("pass") === 0).createOrReplaceTempView("cust")
    jdf0 = spark.sql("select l.cid,c.name,l.lid,l.type from loan l LEFT JOIN cust c on (l.cid=c.cid)")
    jdf0.show


    cust.filter(col("pass") === 1).createOrReplaceTempView("cust")

    jdf0.createOrReplaceTempView("LOAN")
    spark.sql("select l.* from LOAN l LEFT JOIN cust c on (l.cid=c.cid) ").show
    loan.join(cust, Seq("cid"), "left").show

  }
}
