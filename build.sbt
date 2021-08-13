name := "scala-kafka-avro-tutorial"

version := "0.1"

scalaVersion := "2.13.6"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.10.2",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "io.confluent" % "kafka-avro-serializer" % "6.2.0",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.10",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-simple" % "1.7.32"
)

avroStringType := "String"

// The following 2 commands won't work
//avroSource := baseDirectory.value / "resources/avro"
//avroSource := file("src/main/resources/avro")

// Only this one generates the code.
(Compile / avroSource) := file("src/main/resources/avro")
