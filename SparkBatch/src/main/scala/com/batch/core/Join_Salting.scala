package com.batch.core

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Rand
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random

/*
 * Salting Technique
 * This is used to resolve skewness issue in joining.
 * Where all key reside in same task and task will take lot of time to complete as compare to other task.
 */
object Join_Salting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val cust = spark.read.option("header","true").schema(custSchema)
      .csv("SparkBatch/testData/input/interactive_broadcast/Customer.txt").withColumn("pass",col("id")%3).cache()

    val loan = spark.read.option("header","true").schema(loanSchema)
      .csv("SparkBatch/testData/input/interactive_broadcast/Loan.txt").cache()

    cust.createOrReplaceTempView("CUST")
    loan.createOrReplaceTempView("LOAN")

    spark.sql("select l.cid,c.name,l.lid,l.type from LOAN l left join CUST c on (l.cid=c.cid)").show

    ////////SALTING

    val df1 = loan.withColumn("cid",concat(col("cid") , lit("&"),lit(floor(rand(12345654)*10))))
      .repartition(10,col("cid"))
    println("test")

    df1.mapPartitions(itr=>(
     // println("partitions.....")
      itr.toList.map(x=> {println(x)
    x}).iterator))(RowEncoder(loanSchema)).show

    val df2 = df1.withColumn("cid",split(col("cid"),"&")(0))//.repartition(10,col("cid"))


    df2.mapPartitions(itr=>(
      // println("partitions.....")
      itr.toList.map(x=> {println(x)
        x}).iterator))(RowEncoder(loanSchema)).show

    df2.printSchema()

    df2.createOrReplaceTempView("LOAN_LOAN")
    println("######################################################################")
    val df3 = spark.sql("select l.cid,c.name,l.lid,l.type from LOAN_LOAN l left join CUST c on (l.cid=c.cid)")
    show_test(df3)
  }

  def show_test(df:DataFrame):Unit={
    df.mapPartitions(itr=>(
      // println("partitions.....")
      itr.toList.map(x=> {println(x)
        x}).iterator))(RowEncoder(joinSchema)).show
  }

  val custSchema:StructType = StructType(List(new StructField("id",IntegerType,false),
    new StructField("cid",StringType,false)
    ,new StructField("name",StringType,false)))


  val loanSchema:StructType = StructType(List(new StructField("id",IntegerType,false),
    new StructField("cid",StringType,false)
    ,new StructField("lid",StringType,false)
    ,new StructField("type",StringType,false)))

  val joinSchema:StructType = StructType(List(new StructField("cid",StringType,false),
    new StructField("name",StringType,false)
    ,new StructField("lid",StringType,false)
    ,new StructField("type",StringType,false)))

}
