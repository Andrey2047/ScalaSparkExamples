package com.ak.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameTest {

  val postSchema = StructType(Seq(
    StructField("commentCount", IntegerType, true),
    StructField("lastActivityDate", TimestampType, true),
    StructField("ownerUserId", LongType, true),
    StructField("body", StringType, true),
    StructField("score", IntegerType, true),
    StructField("creationDate", TimestampType, true),
    StructField("viewCount", IntegerType, true),
    StructField("title", StringType, true),
    StructField("tags", StringType, true),
    StructField("answerCount", IntegerType, true),
    StructField("acceptedAnswerId", LongType, true),
    StructField("postTypeId", LongType, true),
    StructField("id", LongType, false))
  )



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val itPostsRows = sc.textFile(Consts.FPATH + "/ch05/italianPosts.csv")
    val itPostsSplit = itPostsRows.map(x => x.split("~"))
    val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),
      x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))

    val itPostsDFrame = itPostsRDD.toDF()

    //itPostsDFrame.show(10)

    val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate",
      "ownerUserId", "body", "score", "creationDate", "viewCount", "title",
      "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")

    val postsIdBody = itPostsDF.select("id", "body")

    val countItaliano = postsIdBody.filter('body contains "Italiano").count

    val noAnswer = itPostsDF.filter(('postTypeId === 1)) //and
     // ('acceptedAnswerId isNull))

    noAnswer.show(10)

  }

}
