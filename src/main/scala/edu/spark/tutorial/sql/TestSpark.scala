package edu.spark.tutorial.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hastimal on 10/9/2015.
 */
object TestSpark {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestSpark").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val jsonRD = sqlContext.jsonFile("src/main/resources/inputFile/TweetFile.json")

    jsonRD.registerTempTable("TempTable")

    val textRDD = sqlContext.sql("select text from TempTable")

    val tempRDD = textRDD.map(l => l.toString().split(" ")).map(l => (l, 1)).reduceByKey(_ + _)

    textRDD.save("")
  }
}