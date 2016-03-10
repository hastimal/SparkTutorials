package edu.spark.tutorial.basics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hastimal on 3/7/2016.
 */
object Count {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile("src/main/resources/inputFile/test")

    // Split into words and remove empty lines
    val tokenized = input.map(line => line.split(" ")).filter(words => words.size > 0)
    // Extract the first word from each line (the log level) and do a count
    val counts = tokenized.map(words => (words(0), 1)).reduceByKey{ (a, b) => a + b }
    counts.collect().foreach(println(_))
  }

}
