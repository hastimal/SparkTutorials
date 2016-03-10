package edu.spark.tutorial.sql

/**
 * Created by hastimal on 3/9/2016.
 */
object SQLPracticeDemo {
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val jsonTextFile = sc.textFile("src/main/resources/TweetFile.json").filter(l => l != "")

    val jsonFile = sqlContext.jsonRDD(jsonTextFile)

    //Creating and registering main table

    jsonFile.registerTempTable("MainTable")

    // Retrieving the user details from main table

    val userTable = sqlContext.sql("select user.id, user.lang, user.name, user.followersCount, user.friendsCount, user.location, user.description from MainTable")

    //saving user details data as paraquet file to create a separate table.

    val UserTable = userTable.saveAsParquetFile("src/main/resources/UserTable/UserDetails")


    //Loading paraquet file to create table.

    val UserDetails = sqlContext.parquetFile("src/main/resources/UserTable/UserDetails")

    //Creating table with user details.

    UserDetails.registerTempTable("UserDetails")

    // query to retrieve the user details

    val username = sqlContext.sql("SELECT lang, count(*) AS lang_count FROM UserDetails GROUP BY lang ORDER BY lang_count DESC")

    // printing details.

    username.foreach(println)

    //Retrieving the hash tags from hash tags table.

    val hashTages = sqlContext.sql("SELECT id,createdAt,hashtagEntities.text as hashTagLine FROM MainTable where hashtagEntities.text IS NOT NULL")

    //Saving the data as paraquet file to create table

    hashTages.saveAsParquetFile("src/main/resources/hashTags")

    //loading paraquet file to create table hashTags table

    val HashTagsTable = sqlContext.parquetFile("src/main/resources/hashTags")

    // registering or creating the HashTagsTable table

    HashTagsTable.registerTempTable("HashTagsTable")

    //retrieving the data from hashTags to split into words

    val hashTagDetails = sqlContext.sql("SELECT  hashTagLine from HashTagsTable").map(l => l.getList(0))

    //converting into single array to split into words

    val htd = hashTagDetails.filter(l => l.size() > 0).flatMap(tags => {
      tags.toArray
    })

    // Saving hashtags into text file to create table

    htd.saveAsTextFile("src/main/resources/hashTagsText")

    //Loading the text file.

    val hashTagsTextFile = sc.textFile("src/main/resources/hashTagsText")

    //variable holding the schema name to apply for data frame

    val schemaString = "HashTags "

    //Creating the schema

    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //Converting RDD into row format to create table

    val rowRDD = hashTagsTextFile.map(r=> r.split(",") ).map(l => Row(l(0)))

    //Creating data frame with RDD and created schema

    val hashTagDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Creating table from data frame created

    hashTagDataFrame.registerTempTable("HashTagsTable")

    //retrieving the data from the table

    val htags = sqlContext.sql("SELECT HashTags, count(*) as hash_count from HashTagsTable group by HashTags order by hash_count DESC")

    //Printing the results of the table.

    htags.foreach(println)

    //retrieving the details from main table

    val table1 = sqlContext.sql("select inReplyToUserId, id, user.id as user_id, user.timeZone as tzone from MainTable ")

    //Saving the data as paraquet file

    table1.saveAsParquetFile("src/main/resources/table1")

    // loading paraquet file.

    val tabletable1 = sqlContext.parquetFile("src/main/resources/table1")

    //Registering the temp table1

    tabletable1.registerTempTable("Table1")

    //retrieving the details from main table.

    val table2 = sqlContext.sql("select user.id as user_id, hashtagEntities.text as hashTags, createdAt from MainTable ")

    //saving the data as paraquet file

    table2.saveAsParquetFile("src/main/resources/table2")

    //loading the data from paraquet file to crete a table.

    val tabletable2 = sqlContext.parquetFile("src/main/resources/table2")

    //registering the data as temp table

    tabletable2.registerTempTable("Table2")

    // retrieving details by apply join condition

    val joinResults = sqlContext.sql("select Table1.id, Table2.user_id, Table2.hashTags, Table2.createdAt, Table1.tzone from Table1 join Table2 on (Table1.inReplyToUserId = Table2.user_id)")

    // saving the data as paraquet file.

    joinResults.saveAsParquetFile("src/main/resources/SQLJoin")

    //printing join results.

    joinResults.foreach(println)

    //retrieving the data from the main table

    val followersdetails = sqlContext.sql("select id, retweetCount, user.followersCount as followCount, text from MainTable")

    //saving the data as paraquet file to create table

    followersdetails.saveAsParquetFile("src/main/resources/followersdetails")

    //loading the paraquet file created

    val followerstable = sqlContext.parquetFile("src/main/resources/followersdetails")

    // Registering the temporary table

    followerstable.registerTempTable("FollowersTable")

    //retrieving the data from followers table

    val followersResults = sqlContext.sql("select id, followCount from FollowersTable order by followCount desc")

    // printing the results.

    followersResults.foreach(println)

    val reTweetDetails = sqlContext.sql("select id, retweetCount from FollowersTable order by retweetCount desc ")

    reTweetDetails.foreach(println)


  }
}
