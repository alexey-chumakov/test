name := "test"

version := "0.1"

scalaVersion := "2.12.10"

fork := true

val Spark = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark
)

Compile / run / mainClass := Some("test.TestDistances")