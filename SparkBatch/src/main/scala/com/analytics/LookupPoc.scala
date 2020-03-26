package com.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * We can analyze below thing from input dataset
  * 1. how many vroot each user has visited
  * 2. Top 10 user who have visited maximum vroot
  * 3. Find out user who had visited atleast 10 vroot
  * 4. which vroot was visited maximum user
  * 5. which vroot was visited minimum user
  */
object LookupPoc {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Lookup POC").getOrCreate()

    val inputText = spark.sparkContext.textFile("testData/input/anonymous-msweb.data")
    val attributeDf = getAttributeDf(spark, inputText)
    val vrootDF = getInputDataFrame(spark, inputText) // "userId", "VrootId"

    attributeDf.show(30)
    //vrootDF.show(30)

    eachUserVisitedVrootCount(spark,vrootDF)
    top_10_User_have_maximum_visited_vroot(spark,vrootDF)

  }

  def top_10_User_have_maximum_visited_vroot(spark: SparkSession, vrootDF: DataFrame): Unit = {

  }


  def eachUserVisitedVrootCount(spark: SparkSession, vrootDF: DataFrame): Unit = {
    vrootDF.groupBy(col("userId")).count().show()
  }

  def getAttributeDf(spark: SparkSession, inputText: RDD[String]) = {
    val attributeLine = inputText.filter(line => line.split(",")(0) == "A")
      .map(row => {
        val id = row.split(",").toSeq
        Row.fromSeq(Seq(id(1), id(3).replace("\"", ""), id(4).replace("\"", "")))

      })

    val schema = StructType(List(new StructField("VrootId", StringType, false),
      new StructField("VrootTitle", StringType, false),
      new StructField("url", StringType, false)
    ))

    val attributeDF = spark.createDataFrame(attributeLine, schema)
    // spark.sparkContext.broadcast(attributeDF)
    attributeDF

  }

  def getInputDataFrame(spark: SparkSession, inputText: RDD[String]) = {

    val result = inputText.filter(_.split(",")(0) != "A").mapPartitions(iterator =>
      new Iterator[String] {

        var result = ""
        var itr_result = ""
        var flag = false
        var lastLine = false
        var oneExec = true

        override def hasNext: Boolean = {

          if (iterator.hasNext)
            true
          else {
            if (oneExec)
              lastLine = true
            else
              lastLine = false
            lastLine
          }
        }

        override def next(): String = {
          if (!lastLine) {
            val line = iterator.next()

            if (line.split(",")(0) == "C") {
              if (result.split(",")(0) == "C") {
                itr_result = result
                flag = true
              }
              result = line.split(",")(0) + "," + line.split(",")(1)
            } else if (line.split(",")(0) == "V") {
              result = result + "," + line.split(",")(1)
            }

            if (flag) {
              flag = false
              itr_result
            } else ""
          } else {
            oneExec = false
            result
          }
        }
      }
    )
    println(result.count())

    //remove empty Row
    import spark.implicits._
    val filter = result.filter(_.length != 0)
      .flatMap(line => {
        val r = line.split(",")
        val userId = r(1).replace("\"", "")
        var list: List[(String, String)] = Nil
        for (i <- 2 to r.size - 1) {
          list = list :+ (userId, r(i))
        }
        list
      }).toDF("userId", "VrootId")

    filter
  }
}