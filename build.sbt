name := "SparkTutorials"

version := "1.0"

scalaVersion := "2.11.7"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0",
"org.apache.spark" %% "spark-sql" % "1.4.0",
"org.apache.spark" %% "spark-hive" % "1.4.0")