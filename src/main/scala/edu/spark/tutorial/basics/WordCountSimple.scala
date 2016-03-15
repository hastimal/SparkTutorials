package edu.spark.tutorial.basics

import java.util.Calendar

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hastimal on 3/14/2016.
 */
object WordCountSimple {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCountSimple <inputPath> <outputPath>")
      System.exit(1)
    }
    val inputPath = args(0)   //input path as variable argument
    val outputPath = args(1)  //output path as variable argument
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("WordCountSimple")
    val sc = new SparkContext(conf)
    val startTime = Calendar.getInstance().getTime()
    println("startTime "+startTime)
    val input = sc.textFile(inputPath,8)
      // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.saveAsTextFile(outputPath)
    counts.foreach(println(_))
    val endTime = Calendar.getInstance().getTime()
    println("endTime "+endTime)
    val totalTime = endTime.getTime-startTime.getTime
    println("totalTime "+totalTime)
  }
}
