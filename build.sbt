name := "spark_learning_demo"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.10.0",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.0-cdh5.10.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0-cdh5.10.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0-cdh5.10.0"
)