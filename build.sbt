name := "project_1"

version := "0.1"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
  "org.apache.spark" %% "spark-hive" % "2.3.1"
)