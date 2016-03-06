package edu.spark.tutorial.basics
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
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile("src/main/resources/inputFile/test")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("src/main/resources/outputFile/testWC")
  }
}
