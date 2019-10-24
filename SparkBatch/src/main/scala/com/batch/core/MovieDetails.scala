package com.batch.core

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}

object MovieDetails {


  def saveToParquet(save: Dataset[Row]): Unit = save.write.parquet("testData/output/Movie/movie_clean")

  def saveSecondHighestStar_rating(spark: SparkSession, df: Dataset[Row], crime: String): Unit = {
    import spark.implicits._
    val win = Window.partitionBy("genre").orderBy(desc("star_rating"))
    val df1 = df.filter($"genre" === crime)
      .withColumn("rank", rank().over(win))
      .filter($"rank" === 2)
      .drop("rank")

    df1.write.format("json").save("testData/output/Movie/secondHighestStarRating")

  }

  def SaveNotRatedMovie(spark: SparkSession, df: Dataset[Row]): Unit = {
    import spark.implicits._
    df.filter($"content_rating" === "Not Rated")
      .write.format("json").mode(SaveMode.Overwrite).save("testData/output/Movie/NonRatedMovie")
  }

  def SaveRandPGHaveLongestDuration(spark: SparkSession, df: Dataset[Row]): Unit = {
    import spark.implicits._

    /* val contents:List[String] = List("R","PG-13")
     val conDF = df.filter($"content_rating".isin(contents:_*)).show
     Or*/

    val conDF = df.filter($"content_rating" === "R" || $"content_rating".startsWith("PG"))
    // conDF.groupBy("content_rating").max("duration").show // give output as content_rating|max(duration) column
    val wind = Window.partitionBy("content_rating").orderBy(desc("duration"))
    val save = conDF.withColumn("rank", rank().over(wind))
      .filter($"rank" === 1)
      .drop("rank")

    save.write.format("json").mode(SaveMode.Overwrite).save("testData/output/Movie/RandPGHaveLongestDuration")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Movie").getOrCreate()

    val df = spark.read.schema(schema).option("header", true).csv("SparkBatch/testData/input/Movie");
    //df.show()
    val save = df.map(row => {
      val str = row.getString(5)
      val actors: Array[String] = str.substring(1, str.length - 1).split(",").map(x => x.substring(2, x.length - 1))
      Row(row.getDouble(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4), actors)
    })(RowEncoder(cleanSchema))

    saveToParquet(save)

    saveSecondHighestStar_rating(spark, save, "Crime")

    SaveNotRatedMovie(spark, save)

    SaveRandPGHaveLongestDuration(spark, save)


  }

  def cleanSchema: StructType = StructType(List(
    new StructField("star_rating", DoubleType, false),
    new StructField("title", StringType, false),
    new StructField("content_rating", StringType, false),
    new StructField("genre", StringType, false),
    new StructField("duration", IntegerType, false),
    new StructField("actors_list", ArrayType(StringType, false), false)
  ))

  def schema: StructType = StructType(List(
    new StructField("star_rating", DoubleType, false),
    new StructField("title", StringType, false),
    new StructField("content_rating", StringType, false),
    new StructField("genre", StringType, false),
    new StructField("duration", IntegerType, false),
    new StructField("actors_list", StringType, false)
  ))
}
