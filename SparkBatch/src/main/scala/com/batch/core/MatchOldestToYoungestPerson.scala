package com.batch.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc_nulls_last, col, desc, row_number}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Input
 * A1,ADULT,58
 * A2,ADULT,54
 * A3,ADULT,53
 * A4,ADULT,54
 * A5,ADULT,51
 * C1,CHILD,16
 * C2,CHILD,15
 * C3,CHILD,17
 * C4,CHILD,18
 *
 *
 * Ouput:
 * older younger
 * A1   C2
 * A2   C1
 * A4   C3
 * A3   C4
 * A5
 *
 * Hint
 * age older younger
 * 58 A1   C2       15
 * 54 A2   C1       16
 * 54 A4   C3       17
 * 53 A3   C4       18
 * 51 A5
 */
object MatchOldestToYoungestPerson {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("Mapping of older with younger").getOrCreate()

    val sch: StructType = StructType(List(new StructField("id", StringType, false),
      new StructField("type", StringType, false),
      new StructField("age", IntegerType, false)))
    val df = spark.read.option("header",false).schema(sch).csv("SparkBatch/testData/input/MatchOlderstToYoungest")
  //  df.show

    val adultDf=df.filter(col("type")==="ADULT")
                  .withColumn("orderage",row_number.over(Window.partitionBy("type").orderBy(desc("age"))))  // row_number work on window so window is required even it is not using in this scenario

    adultDf.show
    /**
     * +---+-----+---+--------+
     *  | id| type|age|orderage|
     *  +---+-----+---+--------+
     *  | A1|ADULT| 58|       1|
     *  | A2|ADULT| 54|       2|
     *  | A4|ADULT| 54|       3|
     *  | A3|ADULT| 53|       4|
     *  | A5|ADULT| 51|       5|
     *  +---+-----+---+--------+
     */

    val childDf=df.filter(col("type")==="CHILD")
      .withColumn("orderage",row_number().over(Window.partitionBy("type").orderBy("age")))
    childDf.show()

    /**
     * +---+-----+---+--------+
     *  | id| type|age|orderage|
     *  +---+-----+---+--------+
     *  | C2|CHILD| 15|       1|
     *  | C1|CHILD| 16|       2|
     *  | C3|CHILD| 17|       3|
     *  | C4|CHILD| 18|       4|
     *  +---+-----+---+--------+
     */

    val result=adultDf.as("ad").join(childDf.as("ch"),Seq("orderage"),"left")
      //.select("ad.id","ch.id")
      .select(col("ad.id").as("older"),col("ch.id").as("Younger"))
    result.show

    /**
     * +-----+-------+
     *  |older|Younger|
     *  +-----+-------+
     *  |   A1|     C2|
     *  |   A2|     C1|
     *  |   A4|     C3|
     *  |   A3|     C4|
     *  |   A5|   null|
     *  +-----+-------+
     */
    /*
     import org.apache.spark.sql.Encoders
     case class Person(id:String,Type:String,age:Integer)

     val enc = Encoders.product[Person]
     val data=Seq(
       Person("A1", "ADULT",58),
       Person("A2", "ADULT", 54),
       Person("A3", "ADULT", 53),
       Person ("A4", "ADULT", 54),
       Person("A5", "ADULT", 51),
       Person ("C1", "CHILD", 16),
       Person("C2", "CHILD", 15),
       Person("C3", "CHILD", 17),
       Person("C4", "CHILD", 18))

     val spark=SparkSession.builder().master("local").appName("Mapping of older with younger").getOrCreate()

     import spark.implicits._

     val datadf=spark.createDataset(data)(enc)
     datadf.show()
     */

  }
}
