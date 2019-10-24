package com.batch.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SCDTwoImplementation {

  def main(args: Array[String]): Unit = {

    val sp = SparkSession.builder().master("local").appName("SCD2").getOrCreate()

    val orginal = sp.read.option("header","true").csv("SparkBatch/testData/input/scd2/orginal")
      .withColumn("eff_start_date", current_timestamp())
      .withColumn("eff_end_date", lit("9999-01-01 00:00:00"))
      .withColumn("curr_ind", lit("Y"))
      .withColumn("md5_chk_sum",lit("MD1"))

    import sp.implicits._

    val delta = sp.read.option("header","true").csv("SparkBatch/testData/input/scd2/delta")
    val originalMd5 = orginal.withColumn("original_non_key_concat",concat_ws("_",
    $"id",$"name",$"address",$"pin"))
      .withColumn("orginal_non_key_md5",sha2($"original_non_key_concat",256))
      .drop($"original_non_key_concat")

    val deltaMd5 = delta.withColumn("delta_non_key_concat",concat_ws("_",
      $"id",$"name",$"address",$"pin"))
      .withColumn("delta_non_key_md5",sha2($"delta_non_key_concat",256))
      .drop($"delta_non_key_concat")

    val incrementalJoin = originalMd5.as("df1").join(deltaMd5.as("df2"),$"df1.id"===$"df2.id","full_outer")
        .withColumn("md5Flag",
          when(($"df1.id"===$"df2.id") && ($"orginal_non_key_md5"===$"delta_non_key_md5"),lit("NCR"))
          .when($"df1.id".isNotNull && $"delta_non_key_md5".isNull, lit("NCR"))
            .when(($"df1.id"===$"df2.id")&&($"orginal_non_key_md5" != $"delta_non_key_md5"),lit("U"))
            .when($"df1.id".isNull && $"delta_non_key_md5".isNotNull, lit("I"))
            )

    incrementalJoin.cache()

    val resultDf1= incrementalJoin.filter($"md5Flag"==="NCR" || $"md5Flag"==="U")
        .select($"df1.*",$"md5Flag")
        .withColumn("eff_end_date",when($"md5Flag"==="U",current_timestamp())
          .otherwise(lit("9999-01-01 00:00:00")))
      .withColumn("curr_ind",when($"md5Flag"==="U",lit("N")).otherwise(lit("Y")))
        .drop("md5Flag","orginal_non_key_md5")

    resultDf1.show

    val resultInsertDf2 = incrementalJoin.filter($"md5Flag"==="I" || $"md5Flag"==="U")
      .select($"df2.*")
        .withColumn("eff_start_date",current_timestamp())
        .withColumn("eff_end_date",lit("9999-01-01 00:00:00"))
      .withColumn("curr_ind",lit("Y"))
      .withColumn("md5_chk_sum",lit("MD1"))
        .drop("delta_non_key_md5")

    val result = resultDf1.union(resultInsertDf2)
    result.show
  }
}

