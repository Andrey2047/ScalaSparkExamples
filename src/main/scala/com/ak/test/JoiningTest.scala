package com.ak.test

import org.apache.spark.sql.SparkSession

object JoiningTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    val sc = spark.sparkContext

    val tranFile = sc.textFile(Consts.FPATH + "/ch04/ch04_data_transactions.txt")
    val transByProd = tranFile.map(line => line.split("#")).map(tran => (tran(3).toInt, tran))

    val totalByProducts = transByProd.mapValues(t => t(5).toDouble).
      reduceByKey{
        case(tot1, tot2) => {tot1 + tot2}
      }

    val products = sc.textFile(Consts.FPATH + "/ch04/"+ "ch04_data_products.txt").
      map(line => line.split("#")).
      map(p => (p(0).toInt, p))

    val totalsAndProds = totalByProducts.join(products)

    val prodTotCogroup = totalByProducts.cogroup(products)

    prodTotCogroup.filter(x => x._2._1.isEmpty).
      foreach(x => println(x._2._2.head.mkString(", ")))



  }

}
