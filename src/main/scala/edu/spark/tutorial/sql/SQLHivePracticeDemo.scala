package edu.spark.tutorial.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by hastimal on 3/9/2016.
 */
object SQLHivePracticeDemo {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local")

    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)

    val sqlContext = new SQLContext(sc)

    //loading the JSON FIle

    //val jsonTextFile = sc.textFile("src/main/resources/TweetFile.json")


    val inputJson = hc.jsonFile("src/main/resources/inputFile/TweetFile.json")

    inputJson.saveAsTable("mainTable")



    /*val inputJson = sqlContext.jsonRDD(jsonTextFile)

    inputJson.registerTempTable("TweetTable")



    println("-------------printing table schemaa--------------")

    inputJson.printSchema()

    val userTable = sqlContext.sql("select user from TweetTable where user.id <> '' ")

    //val (userId,username) = userTable.map( Row(r => r.getLong(0),r.getString(1)))

   val userJson =  userTable.toJSON

    userTable.schema*/





  }
}
