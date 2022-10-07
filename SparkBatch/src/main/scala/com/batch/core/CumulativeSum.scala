package com.batch.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.{orderBy, partitionBy}
import org.apache.spark.sql.functions.{col, sum, to_date, when}

/*
input:
transaction_id|type|amount|transaction_date
19153|deposit|65.90|07/10/2022 0:00:00
53151|deposit|178.55|07/08/2022 10:00:00
29776|withdrawal|25.90|07/08/2022 10:00:00
16461|withdrawal|45.99|07/08/2022 10:00:00
77134|deposit|32.60|07/10/2022 10:00:00
 */
object CumulativeSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val df = spark.read.option("delimiter","|")
      .option("inferSchema","true")
      .option("header","true")
      .option("dateFormat","dd/MM/yyyy HH:mm:ss")  // not working
      .csv("SparkBatch/testData/input/cumulative")

    val cleanDf=df.withColumn("amount",when(col("type")==="withdrawal", col("amount")*(-1.0))
                                    .otherwise(col("amount")))
      .withColumn("transaction_date",to_date(col("transaction_date"),"dd/MM/yyyy"))

    val sumDf=cleanDf.groupBy("transaction_date").agg(sum("amount").as("amount"))
    /*
    +----------------+-----------+
    |transaction_date|amount|
    +----------------+-----------+
    |      2022-08-07|     106.66|
    |      2022-10-07|       98.5|
    +----------------+-----------+
     */
  //  sumDf.show
    val w =Window//.partitionBy("transaction_date")
      .orderBy("transaction_date")
    .rowsBetween(Long.MinValue,0)//.rowsBetween(Window.unboundedPreceding,Window.currentRow)
    sumDf.withColumn("cum_amount",sum("amount").over(w)).show

    // OR
    sumDf.createOrReplaceTempView("sum_temp")
    spark.sql("select transaction_date, amount, sum(amount) over( order by transaction_date) as cum_amount from sum_temp").show

    // OR
    cleanDf.createOrReplaceTempView("clean_temp")
    spark.sql("select transaction_date, amount, sum(amount) over(order by transaction_date) as cum_amount from " +
      "(select transaction_date, sum(amount) as amount from clean_temp group by transaction_date ) ").show

    /*
    +----------------+------+----------+
    |transaction_date|amount|cum_amount|
    +----------------+------+----------+
    |      2022-08-07|106.66|    106.66|
    |      2022-10-07|  98.5|    205.16|
    +----------------+------+----------+
     */

    cleanDf.createOrReplaceTempView("clean")
    spark.sql(
      "select *, sum(amount) over (partition by transaction_date order by transaction_id ) as cum_amount " +
        "from clean ").show


    // OR
    /*
    val w =Window.partitionBy("transaction_date")
      .orderBy("transaction_id")
    .rowsBetween(Long.MinValue,0)//.rowsBetween(Window.unboundedPreceding,Window.currentRow)
    cleanDf.withColumn("cum_amount",sum("amount").over(w)).show
    +--------------+----------+------+----------------+------------------+
    |transaction_id|      type|amount|transaction_date|        cum_amount|
    +--------------+----------+------+----------------+------------------+
    |         16461|withdrawal|-45.99|      2022-08-07|            -45.99|
    |         29776|withdrawal| -25.9|      2022-08-07|            -71.89|
    |         53151|   deposit|178.55|      2022-08-07|106.66000000000001|
    |         19153|   deposit|  65.9|      2022-10-07|              65.9|
    |         77134|   deposit|  32.6|      2022-10-07|              98.5|
    +--------------+----------+------+----------------+------------------+


     */
  }
}
