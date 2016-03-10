package edu.spark.tutorial.advanced

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hastimal on 3/7/2016.
 */
object JsonQuery {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // initialise spark context
    val conf = new SparkConf().setAppName("JsonQuery").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val tweets = sqlContext.jsonFile("src/main/resources/inputFile/tweet").registerTempTable("tweets")

    val results = sqlContext.sql("SELECT user.name, text FROM tweets")
    results.foreach(println(_))
  }
}
