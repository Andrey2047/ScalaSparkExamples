package com.ak.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.StatCounter
import scala.io.Source.fromFile

object ReadJsonExample {

  def main(args: Array[String]): Unit = {
    val logFile = "files/*.json" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    val ghLog = spark.read.json(logFile)
    import spark.implicits._

    val pushes = ghLog.filter("type = 'PushEvent'")

    pushes.printSchema
    println("all events: " + ghLog.count)
    println("only pushes: " + pushes.count)
    pushes.show(5)

    val grouped = pushes.groupBy("actor.login").count()
    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val empPath = "files/employees.txt"
    val employees = Set() ++ (
      for {
             line <- fromFile(empPath).getLines
           } yield line.trim
      )

    val bcEmployees = spark.sparkContext.broadcast(employees)
    val isEmp: (String => Boolean) = (arg: String) => bcEmployees.value.contains(arg)

    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()

  }

}
