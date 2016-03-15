import sbt.Keys._

lazy val root = (project in file(".")).
  settings(
    name := "SparkTutorials",
    version := "1.0",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("WordCountSimple")
  )

exportJars := true
fork := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0",
"org.apache.spark" %% "spark-sql" % "1.4.0",
"org.apache.spark" %% "spark-hive" % "1.4.0")

assemblyJarName := "WordCountSimple.jar"
//
val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}