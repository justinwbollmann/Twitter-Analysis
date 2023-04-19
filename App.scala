package edu.ucr.cs.cs167.jboll005

import jdk.nashorn.internal.parser.Lexer.RegexToken
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType.SQL
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array, col, column}
import org.apache.spark.sql.functions.{split, explode, count}


object App {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Task 1D")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val inputFile: String = args(0)
    try {
      var validOperation = true

      //loading json file
      val twitterDF = sparkSession.read.format("json")
        .option("sep", "\t")
        .option("InferSchema", "true")
        .option("header", "true")
        .load(inputFile)

       // twitterDF.printSchema() <--- used this to figure out attribute names

      //Showing the top 20 results of each attributes.
      val listCols = List("text", "id", "reply_count", "retweet_count", "quoted_status_id", "entities.hashtags", "user.description")
      twitterDF.select(listCols.map(m=>col(m)):_*).show()

      //exploding hashtags in order and limiting to 20, AND in descending order
      val hashtagDF = twitterDF.select(explode(col("entities.hashtags.text")).as("hashtags"))
      val hashtagList = hashtagDF.select("hashtags").rdd.map(r=> r(0).toString).collect.toList
      val top20 = hashtagDF.groupBy("hashtags").count().orderBy(col("count").desc).limit(20)
      top20.show()

    } finally {
      sparkSession.stop()
    }
  }
}