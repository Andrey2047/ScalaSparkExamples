package com.ak.test

import org.apache.spark.sql.SparkSession

object PairRDDTest {

  def main(args: Array[String]): Unit = {
    val logFile = "files/*.json" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    val sc = spark.sparkContext

    val tranFile = sc.textFile(Consts.FPATH + "/ch04/ch04_data_transactions.txt")
    val tranData = tranFile.map(_.split("#"))
    var transByCust = tranData.map(tran => (tran(2).toInt, tran))

    println(transByCust.countByKey())
    transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))

    transByCust = transByCust.flatMapValues(tran => {
      if(tran(3).toInt == 81 && tran(4).toDouble >= 5) {
        val cloned = tran.clone()
        cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";
        List(tran, cloned)
      }
      else
        List(tran)
    })

    val amounts = transByCust.mapValues(t => t(5).toDouble)
    val totals = amounts.foldByKey(0)((p1, p2) => p1 + p2).collect()

    println(totals.sortBy(_._2).last)
  }

}
