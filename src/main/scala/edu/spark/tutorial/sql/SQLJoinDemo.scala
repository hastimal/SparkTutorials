package edu.spark.tutorial.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hastimal on 3/9/2016.
 */
object SQLJoinDemo {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val jsonTextFile = sc.textFile("src/main/resources/inputFile/TweetFile.json").filter(l => l != "")

    val jsonFile = sqlContext.jsonRDD(jsonTextFile)

    // val mainTable = jsonFile.saveAsTable("MainTable")

    jsonFile.registerTempTable("MainTable")

    val table1 = sqlContext.sql("select inReplyToUserId, id, user.id as user_id, user.timeZone as tzone from MainTable ")

    table1.saveAsParquetFile("src/main/resources/table1")

    val tabletable1 = sqlContext.parquetFile("src/main/resources/table1")

    tabletable1.registerTempTable("Table1")

    val table2 = sqlContext.sql("select user.id as user_id, hashtagEntities.text as hashTags, createdAt from MainTable ")

    table2.saveAsParquetFile("src/main/resources/table2")

    val tabletable2 = sqlContext.parquetFile("src/main/resources/table2")

    tabletable2.registerTempTable("Table2")

    val joinResults = sqlContext.sql("select Table1.id, Table2.user_id, Table2.hashTags, Table2.createdAt, Table1.tzone from Table1 join Table2 on (Table1.inReplyToUserId = Table2.user_id)")

    joinResults.foreach(println)


  }

}
