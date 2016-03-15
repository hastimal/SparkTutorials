package edu.spark.tutorial.basics

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by hastimal on 3/5/2016.
 */
object WordCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","8g")
    val sc = new SparkContext(conf)
    // Load our input data.
//    val input = sc.textFile("src/main/resources/inputFile/test",10)
    val input = sc.textFile(args(0),10)
    println(input.partitions.size)
    val startTime = Calendar.getInstance().getTime()
    println("startTime "+startTime)


    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
//    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    //counts.saveAsTextFile("src/main/resources/outputFile/testWC")
    println("###############ReduceByKey####################")
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.foreach(println(_))

    counts.saveAsTextFile(args(1))

    println("###############groupByKey####################")
    val counts1 = words.map(word => (word, 1)).groupByKey()
    counts1.foreach(println(_))
    println("###############combineByKey####################")
//    val counts2 = words.map(word => (word, 1)).combineByKey{case (x, y) => x + y}
//    counts1.foreach(println(_))
    println("###############countByValue--Fast for WC####################")
    val counts2 = words.map(word => (word, 1)).countByValue()
    counts2.foreach(println(_))


    val endTime = Calendar.getInstance().getTime()
    println("endTime "+endTime)
    val totalTime = endTime.getTime-startTime.getTime
    println("totalTime "+totalTime)
  }
}
