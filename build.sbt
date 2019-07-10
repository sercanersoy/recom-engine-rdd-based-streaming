name := "recom-engine-streaming"
version := "0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
//  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion,
//  "org.apache.kafka" %% "kafka" % "2.3.0",
//  "org.apache.kafka" % "kafka-clients" % "2.3.0"
//)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "2.3.0",
  "org.apache.kafka" % "kafka-clients" % "2.3.0"
)

