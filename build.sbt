name := "streaming_aggregation_poc"

version := "1.0"

scalaVersion := "2.11.8"


resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/",
  "Confluent" at "http://packages.confluent.io/maven/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" ,
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3",
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0" ,
  "org.apache.avro" % "avro" % "1.8.2",
  "com.twitter" % "bijection-avro_2.11" % "0.9.5",
  "com.typesafe" % "config" % "1.3.1",
  "io.confluent" % "kafka-avro-serializer" % "3.2.1",

 "com.fasterxml.jackson.core" % "jackson-core" % "2.8.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5",
 "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.5"
)

javaOptions ++= Seq("-Xms512M",
                    "-Xmx2048M",
                    "-XX:+CMSClassUnloadingEnabled")

mainClass in assembly := Some("com.the_ica.spark.StreamingJob")

assemblyMergeStrategy in assembly := {

  case PathList("netty", "handler", xs@_*) => MergeStrategy.first
  case PathList("netty", "buffer", xs@_*) => MergeStrategy.first
  case PathList("netty", "common", xs@_*) => MergeStrategy.first
  case PathList("netty", "transport", xs@_*) => MergeStrategy.first
  case PathList("netty", "codec", xs@_*) => MergeStrategy.first

  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
