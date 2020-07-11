package com.batch.core

import org.apache.spark.sql.SparkSession
/*
 * In this example data frame have column in small letter and while reading same column in spark sql it is capital letter test
 * As we see in this example it is working fine
 */
object ColumnCase {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

    val empDf = Seq(
      ("e001","d001","electric","AA","Malad","Mumbai"),
      ("e002","d001","textile","BB","Yerwarda","Pune"),
        ("e003","d001","electric","CC","Malad","Mumbai"),
      ("e004","d001","textile","DD","Yerwarda","Pune"),
        ("e005","d002","software","EE","Panjim","Goa"),
      ("e006","d002","hardware","FF","Panjim","Goa")
    ).toDF("eid","did","speclization","ename","address","city")

    val deptDf=Seq(
      ("d001","electric","wiring","cable"),
      ("d001","textile","cement","tiles"),
      ("d002","software","testing","development"),
      ("d002","hardware","installation","setup")
    ).toDF("did","speclization","work1","work2")

    empDf.createOrReplaceTempView("emp")
    deptDf.createOrReplaceTempView("dept")

    val edf =
      spark.sql("SELECT DID ||'~'|| SPECLIZATION || '|' AS PRIMARY_KEY_VAL, ENAME,ADDRESS FROM emp")
    val dedf = spark.sql("SELECT DID ||'~'|| SPECLIZATION || '|' AS PRIMARY_KEY_VAL, WORK1,WORK2 from dept")

    edf.show()
    dedf.show()
  }

}
