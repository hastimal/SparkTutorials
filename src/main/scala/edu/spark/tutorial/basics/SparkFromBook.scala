package edu.spark.tutorial.basics
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by hastimal on 3/5/2016.
 */
object SparkFromBook {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // initialise spark context
    val conf = new SparkConf().setAppName("SparkBasicsFromBook").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    ///
    // do stuff
    println("############RDD Simple operations############")

    val lines = sc.textFile("src/main/resources/inputFile/test") // Create an RDD called lines

    println("Count:NO OF ITEMS IN RDD "+lines.count()) // Count the number of items in this RDD
    println("lines.collect().length IN RDD "+lines.collect().length)
    println("First line of RDD: "+lines.first()) // First item in this RDD, i.e. first line of README.md
    lines.take(3).foreach(println(_))

    //Filtering
    val javaLines = lines.filter(line => line.contains("print"))
    println("First line of RDD: "+javaLines.first())
    javaLines.count()


    sc.stop()
  }
}
