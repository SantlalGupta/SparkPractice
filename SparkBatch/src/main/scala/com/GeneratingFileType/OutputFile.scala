package com.GeneratingFileType

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object OutputFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Users\\Santlal\\hadoop")
    val spark=SparkSession.builder()
      .master("local")
      .config("spark.local.dir", "/tmp/spark-temp")
      .getOrCreate()
    import spark.implicits._
    val df=Seq(
      (1,"2"),
      (2,"3")).toDF("A","B")
    df.write.mode(SaveMode.Overwrite).json("json")
    //Write(df).output("C:\\Users\\Santlal\\Downloads\\orcOut","ORC")
   // Write(df).output("jsonOut","JSON")
    //Write(df).output("parquetOut","PARQUET")


  }

}
case class Write(df:DataFrame) {
  implicit def output(oPath:String,oType:String): Unit = {
    oType match {
      case "ORC" => df.write.mode(SaveMode.Overwrite).orc(oPath)
      case "JSON" => df.write.mode(SaveMode.Overwrite).json(oPath)
      case "PARQUET" => df.write.mode(SaveMode.Overwrite).parquet(oPath)
      case _=> println("Incorrect type")
    }
  }
}
