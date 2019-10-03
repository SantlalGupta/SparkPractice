package com.streaming.kafka

import org.apache.spark.sql.SparkSession

object KafkaIntegration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("kafka integration").getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "first_topic1")
      .load()


    val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df1.printSchema()


    //df.select(get_json_object("$key").cast("String"),get_json_object("$value").cast("String"))
    val query = df1.writeStream
      .outputMode("append")
      .format("console")
      // .trigger(ProcessingTime("25 seconds"))
      .start()
    query.awaitTermination()
    /*     val ds = df
       .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
       .writeStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
       .option("topic", "topic1")
       .start()*/


  }

}