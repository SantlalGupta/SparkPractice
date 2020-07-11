package com.batch.core

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object ComparingTwoDF {
  val colForCompare="A,B,C,D"
  val primaryKey="P,K"
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local").appName("comparing two df").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df1:DataFrame=Seq(
      ("A","B","A_1","B_1","C_1"),
      ("C","D","A_11","B_11","C_11")
    ).toDF("P","K","A","B","C")

    val df2:DataFrame = Seq(
      ("A","B","A_2","B_2","D_2"),
      ("C","D","A_22","B_22","D_22")
    ).toDF("P","K","A","B","D")


    getGeneratedDataFrame(spark,df1,colForCompare,primaryKey).show()
    getGeneratedDataFrame(spark,df2,colForCompare,primaryKey).show
  }

  def getGeneratedDataFrame(spark:SparkSession,df:DataFrame,colForCompare:String,primaryKey:String):DataFrame = {
    val dfCol = df.columns.toList
    val temptable = "tempTable"
    val filtCol1 = colForCompare.split(",").filter(col=> !dfCol.contains(col))

    val newColDf=filtCol1.foldLeft(df)((df,col)=> df.withColumn(col,lit("DuMuY_VaL")))
    val query = "Select distinct "+ primaryKey.split(",").mkString("||'~'||") + " || '|' primary_key_val," + colForCompare + " from " + temptable

    newColDf.createOrReplaceTempView(temptable)

    spark.sql(query)
  }
}
