package edu.spark.tutorial.basics


import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by hastimal on 3/5/2016.
 */


  object SparkBasicOperations {
    def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir","F:\\winutils")
      import org.apache.log4j.{Level, Logger}
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      // initialise spark context
      val conf = new SparkConf().setAppName("SparkBasicOperations").setMaster("local[2]").set("spark.executor.memory","4g")
      val sc = new SparkContext(conf)
      ///
      // do stuff
      println("############FlatMap and Map Operation############")
      /**
       * map and flatMap are similar, in the sense they take a line from the input RDD and apply a function on it.
       * The way they differ is that the function in map returns only one element, while function in flatMap can return a
       * list of elements (0 or more) as an iterator.
       Also, the output of the flatMap is flattened. Although the function in flatMap returns a list of elements, the flatMap
       returns an RDD which has all the elements from the list in a flat way (not a list).*/
      /**
       * The input function to map returns a single element, while the flatMap returns a list of elements (0 or more). And also, the output of the flatMap is flattened.*/
      //val lines = sc.textFile("src/main/resources/inputFile/sample")


      //val lines = sc.parallelize(Array("hello world", "hi"))
      val lines = sc.parallelize(List("hello world", "hi")).cache()
      val wordsWithMap = lines.map(line => line.split(" ")).coalesce(1)
      val wordsWithFlatMap = lines.flatMap(line => line.split(" ")).coalesce(1)

      //    wordsWithMap.saveAsTextFile("src/main/resources/wordsWithMap/sampleWordCountOutPut.txt")
      //    wordsWithFlatMap.saveAsTextFile("src/main/resources/wordsWithFlatMap/sampleWordCountOutPut.txt")
      //wordsWithMap.collect().toList.foreach(println(_))
      wordsWithMap.take(2).foreach(println(_))           ///  "hello world", "hi"
      wordsWithFlatMap.foreach(println)                   //hello
      // world",
      // "hi"

      val lines1 = sc.parallelize(List("hello world", "hi")).cache()
      println("#####map#####")
      lines1.map(_.split(" ")).take(2).foreach(println(_))
      println("#####flatMap#####")
      lines.flatMap(_.split(" ")).take(3).foreach(println(_))                      //A flatMap() flattens multiple list into one single List
    }

  //
  //mountain@mountain:~/sbook$ cat words.txt
  //line1 word1
  //  line2 word2 word1
  //line3 word3 word4
  //line4 word1
  //scala>
  //res4: Array[Array[String]] = Array(Array(line1, word1), Array(line2, word2, word1), Array(line3, word3, word4))
  //
  //
  //
  //scala>
  //res5: Array[String] = Array(line1, word1, line2)
}
