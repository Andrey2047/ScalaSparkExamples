package com.ak.test

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark
import org.apache.spark.util.StatCounter




object TT1 {

  def main(args: Array[String]): Unit = {

    //implicit val encoder: Encoder[Array[String]] = Encoders.bean(Array.getClass)
    val logFile = "client-ids.log" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val lines = spark.read.textFile(logFile)
    val idsStr = lines.flatMap(line => line.split(",")).map(_.toDouble).collect()
    println(StatCounter(idsStr).mean)
  }

}
