package com.batch.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * This demonstrate how we can transform new row into new column, I am assumning initial columns are fixed.
 * Input data set:
 * +------+------+------+------+------+
 * |Entity|m1_amt|m2_amt|m3_amt|m4_amt|
 * +------+------+------+------+------+
 * |   ISO|     1|     2|     3|     4|
 * |  TEST|     5|     6|     7|     8|
 * |  Beta|     9|    10|    11|    12|
 * +------+------+------+------+------+
 *
 * As I want to convert column m1_amt,m2_amt,m3_amt and m4_amt as new row so i have done select_expr and used stack
 * inDF.selectExpr("Entity","stack(4,'m1_amt',m1_amt,'m2_amt',m2_amt,'m3_amt',m3_amt,'m4_amt',m4_amt)")
 * output :
 * +------+------+----+
 * |Entity|  col0|col1|
 * +------+------+----+
 * |   ISO|m1_amt|   1|
 * |   ISO|m2_amt|   2|
 * |   ISO|m3_amt|   3|
 * |   ISO|m4_amt|   4|
 * |  TEST|m1_amt|   5|
 * |  TEST|m2_amt|   6|
 * |  TEST|m3_amt|   7|
 * |  TEST|m4_amt|   8|
 * |  Beta|m1_amt|   9|
 * |  Beta|m2_amt|  10|
 * |  Beta|m3_amt|  11|
 * |  Beta|m4_amt|  12|
 * +------+------+----+
 *
 * Now i want to transform above dataframe into final output by,
 * as i want to convert ISO,TEST,Beta as new column so "pivot("Entity")" and grouped col0 as "groupby("col0")"
 * after this out of this group collect_list or collect_set of col1 as ".agg(concat_ws("",collect_list(col("col1")))"
 * This will result new column as col0 so renamed this column as Entity.
 * final code will be :
 * selectDf.groupBy("col0").pivot("Entity").agg(concat_ws("",collect_list(col("col1")))).withColumnRenamed("col0","Entity").show
 * output :
 * +------+----+---+----+
 * |Entity|Beta|ISO|TEST|
 * +------+----+---+----+
 * |m3_amt|  11|  3|   7|
 * |m4_amt|  12|  4|   8|
 * |m2_amt|  10|  2|   6|
 * |m1_amt|   9|  1|   5|
 * +------+----+---+----+
 */
object ConvertRowToColumn_IncludingColumnName {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val inDF = spark.read.schema(schema).option("header","true").csv("SparkBatch/testData/input/RowToColumn_includeColumnName")
    inDF.show

    import org.apache.spark.sql.functions._

    val selectDf= inDF.selectExpr("Entity","stack(4,'m1_amt',m1_amt,'m2_amt',m2_amt,'m3_amt',m3_amt,'m4_amt',m4_amt)")
    selectDf.show
    selectDf.groupBy("col0").pivot("Entity").agg(concat_ws("",collect_list(col("col1")))).withColumnRenamed("col0","Entity").show
  }
  def schema:StructType = StructType(List(
    //Entity,m1_amt,m2_amt,m3_amt,m4_amt
    new StructField("Entity",StringType,false),
    new StructField("m1_amt",IntegerType,false),
    new StructField("m2_amt",IntegerType,false),
    new StructField("m3_amt",IntegerType,false),
    new StructField("m4_amt",IntegerType,false)
  ))

  /*


scala> import org.apache.spark.sql.DataFrame
scala> df.show
+------+------+------+------+------+
|Entity|m1_amt|m2_amt|m3_amt|m4_amt|
+------+------+------+------+------+
|   ISO|     1|     2|     3|     4|
|  TEST|     5|     6|     7|     8|
|  Beta|     9|    10|    11|    12|
+------+------+------+------+------+


scala> def transposeUDF(df: DataFrame, columns: Seq[String]):DataFrame = {
     |  var newdf = spark.emptyDataFrame
     |  columns.foreach{ c =>
     |   val newdf1 = df.groupBy(lit("Entity")).pivot("Entity").agg(concat_ws("",collect_list(col(c)))).withColumn("Entity", lit(c))
     |  if(newdf.count != 0){
     |  newdf = newdf.union(newdf1)}
     |  else { newdf = newdf1}
     |   }
     |   newdf}

scala> val df1 = transposeUDF(df,Seq("m1_amt", "m2_amt", "m3_amt", "m4_amt"))

scala> df1.show
+------+----+---+----+
|Entity|Beta|ISO|TEST|
+------+----+---+----+
|m1_amt|   9|  1|   5|
|m2_amt|  10|  2|   6|
|m3_amt|  11|  3|   7|
|m4_amt|  12|  4|   8|
+------+----+---+----+


   */
}
