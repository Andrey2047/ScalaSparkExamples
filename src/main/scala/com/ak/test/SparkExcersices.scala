package com.ak.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkExcersices {

  case class ExtendedRow(values: String, delimiter: String, newValues: Array[String])

  def main(args: Array[String]): Unit = {
    explodeJson

  }

  private def splitString: Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val dept = Seq(
      ("50000.0#0#0#", "#"),
      ("0@1000.0@", "@"),
      ("1$", "$"),
      ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

    val newSet = dept.map(r => {
      val delimiter = r.getAs[String](1)
      val newStr = r.getAs[String](0).split(delimiter)
      ExtendedRow(r.getAs[String](0), delimiter, newStr)
    })

   // val newSet = dept.withColumn("split_values", split(col("VALUES"), col("Delimiter").)

    newSet.show()
  }

  private def mostImportantRows: Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val input = Seq(
      (1, "MV1"),
      (1, "MV2"),
      (2, "VPV"),
      (2, "Others")).toDF("id", "value")
  }

  private def merge2Rows: Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val input = Seq(
      ("100","John", Some(35),None),
      ("100","John", None,Some("Georgia")),
      ("101","Mike", Some(25),None),
      ("101","Mike", None,Some("New York")),
      ("103","Mary", Some(22),None),
      ("103","Mary", None,Some("Texas")),
      ("104","Smith", Some(25),None),
      ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")

    input.groupBy('id, 'name)
      .agg(first("age", ignoreNulls = true) as "age",
        first("city", ignoreNulls = true) as "city")
      .orderBy("id")
      .show()
  }

  private def explodeJson: Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val openCloseSchema = new StructType().add("close", StringType).add("open", StringType)
    val jsonSchema = new StructType()
      .add("Monday", openCloseSchema)
      .add("Tuesday", openCloseSchema)
      .add("Wednesday", openCloseSchema)
      .add("Thursday", openCloseSchema)
      .add("Friday", openCloseSchema)
      .add("Saturday", openCloseSchema)
      .add("Sunday", openCloseSchema)

    spark.read.format("json")
      .option("multiline", "true")
      .load("input1.json")
      //.withColumn("day", from_json(col("hours"), MapType(StringType,StringType)))
      //.show()
  }
}