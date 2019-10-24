package com.batch.core

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object UniqueRowNumOnEachRowInPartition {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()
    val inputDf = spark.read.schema(getSchema).csv("SparkBatch/testData/input/UniqueRowNumOnEachRowInPartition")
inputDf.show()

    val dateString = getDateString
    val partitionDf = inputDf.repartition(4)

    val rdd =  partitionDf.rdd.mapPartitionsWithIndex( (in, it) => {
      var countR = 0
      new Iterator[Row] {
        override def hasNext: Boolean = it.hasNext

        override def next(): Row = {
          val rowN =  (dateString + in + countR).toLong
          countR +=1
          val list = it.next().toSeq.toList :+ rowN
          Row(list:_*)
        }
      }
    },true
    )

    val rowNumDf = spark.createDataFrame(rdd,getNewSchema)
  //  rowNumDf.show

   rowNumDf.foreach(r=>println(r.toString()))

  }

  def getSchema :StructType = StructType(List (
    new StructField("f1",StringType,false),
    new StructField("f2",StringType,false)
  ))

  def getNewSchema:StructType = StructType(List (
    new StructField("f1",StringType,false),
    new StructField("f2",StringType,false),
    new StructField("row_num",LongType,false)
  ))

  def getDateString:String = {
    val pattern = "yyyyMMddHHmmSS"
    val simpleDate = new SimpleDateFormat(pattern)
    simpleDate.format(new Date)
  }
}
