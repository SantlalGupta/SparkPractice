package com.analytics

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * DataFrame :
  * 1. Attribute            -- attribute
  * 2. user visited vroot   -- user_visited_vroot
  *
  * Analysis of input:
  * 1. how many vroot each user has visited         -- user_visited_vroot_count
  * 2. Top 10 user who have visited maximum vroot   -- top_10_user_visited_maximum_vroot
  * 3. top visited vroot by user                    -- max_visited_vroot
  * 4. least visited vroot by user                  -- least_visited_vroot
  * 5. List of non visited vroot by any user        -- non_visited_vroot_by_user
  * 6. Lookup of individual vroot details           -- select_vroot_details
  *
  * This application used input in below formats
  * <property file name>  <input data set>  <vrootId>
  * example:
  * config.properties SparkBatch/testData/input/analytics/anonymous-msweb.data 1001
  */
object LookupPoc {

  def main(args: Array[String]): Unit = {

    val propertyMap:Map[String,String] = new PropertyLoader().getMapProperties(args(0))

    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","10")
      .master("local[*]").appName("Lookup POC").getOrCreate()

   // val inputText = spark.sparkContext.textFile("testData/input/anonymous-msweb.data")
   val inputText = spark.sparkContext.textFile(args(1))
    val attributeDf = getAttributeDf(spark, inputText,propertyMap)
    val vrootDF = getInputDataFrame(spark, inputText,propertyMap) // "userId", "VrootId"

    eachUserVisitedVrootCount(spark,vrootDF,propertyMap)
    top_10_User_have_maximum_visited_vroot(spark, vrootDF,propertyMap)
    top_and_least_visited_vroot_by_user(spark, vrootDF, attributeDf,propertyMap)
    non_visited_vroot_by_user(spark, vrootDF, attributeDf,propertyMap)

    if(args.length>2){
      lookup_vroot_detail(args(2),vrootDF,attributeDf,propertyMap)
    }
  }

  def lookup_vroot_detail(str: String,vrootDF: DataFrame, attributeDf: DataFrame,propertyMap:Map[String,String]): Unit = {
    val vroot = vrootDF.filter(s"VrootId==$str").groupBy("VrootId").count//.show

    val least_visited_vroot=attributeDf.join(vroot, "VrootId")
    saveToDB(least_visited_vroot,"select_vroot_details",propertyMap)
  }

  def non_visited_vroot_by_user(spark: SparkSession, vrootDF: DataFrame, attributeDf: DataFrame,propertyMap:Map[String,String]): Unit = {
    val visitedvroot = vrootDF.select("VrootId").distinct()

    val broadcastvistedvroot = spark.sparkContext.broadcast(visitedvroot)
    val nonVistedVroot = attributeDf.join(broadcastvistedvroot.value, Seq("VrootId"), "leftanti")

    saveToDB(nonVistedVroot,"non_visited_vroot_by_user",propertyMap)

  }

  def getAttributeDf(spark: SparkSession, inputText: RDD[String],propertyMap:Map[String,String]) = {
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
    saveToDB(attributeDF,"attribute",propertyMap)
    attributeDF

  }

  def getInputDataFrame(spark: SparkSession, inputText: RDD[String],propertyMap:Map[String,String]) = {

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

    saveToDB(filter,"user_visited_vroot",propertyMap)

    filter
  }

  def top_and_least_visited_vroot_by_user(spark: SparkSession, vrootDF: DataFrame, attributeDf: DataFrame,propertyMap:Map[String,String]): Unit = {
    val vrootCount = vrootDF.groupBy(col("VrootId")).count()
    val maxVisited = vrootCount.withColumn("rank", rank().over(Window.orderBy(col("count").desc)))
      .filter("rank<=10")
      .drop("rank")

    val leastVisited = vrootCount.withColumn("rank", rank().over(Window.orderBy(col("count").asc)))
      .filter("rank<=2")
      .drop("rank")

    leastVisited.show(50)

    val broadcastLeastVisited = spark.sparkContext.broadcast(leastVisited)
    val least_visited_vroot=attributeDf.join(broadcastLeastVisited.value, "VrootId")

    val broadcastVisited = spark.sparkContext.broadcast(maxVisited)
    val max_visited_vroot=attributeDf.join(broadcastVisited.value, "VrootId")

    saveToDB(least_visited_vroot,"least_visited_vroot",propertyMap)
    saveToDB(max_visited_vroot,"max_visited_vroot",propertyMap)

  }

  def top_10_User_have_maximum_visited_vroot(spark: SparkSession, vrootDF: DataFrame,propertyMap:Map[String,String]): Unit = {
    val top_10_user_visited_maximum_vroot=vrootDF.groupBy(col("userId")).count() //.orderBy(col("count").desc).show
      .withColumn("rank", rank().over(Window.orderBy(col("count").desc)))
      .filter("rank<=10")
      .drop("rank")



    saveToDB(top_10_user_visited_maximum_vroot,"top_10_user_visited_maximum_vroot",propertyMap)
  }

  def eachUserVisitedVrootCount(spark: SparkSession, vrootDF: DataFrame,propertyMap:Map[String,String]): Unit = {
    val user_visited_vroot_count = vrootDF.groupBy(col("userId")).count()

    saveToDB(user_visited_vroot_count,"user_visited_vroot_count",propertyMap)
  }

  def saveToDB(df:DataFrame,table:String,propertyMap:Map[String,String]) : Unit = {

    val properties = new Properties()

    val mysql = new MysqlProperty()
    properties.setProperty(mysql.DRIVER,propertyMap.get(mysql.DRIVER).get)
    properties.setProperty(mysql.USER,propertyMap.get(mysql.USER).get)
    properties.setProperty(mysql.PASSWORD,propertyMap.get(mysql.PASSWORD).get)

    df.write.mode(SaveMode.Overwrite)
      .option(mysql.BATCHSIZE,propertyMap.get(mysql.BATCHSIZE).get.toInt)
      .jdbc(propertyMap.get(mysql.URL).get,table,properties)
  }

}