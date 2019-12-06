name := "rsvpsproducer"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "2.3.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.1",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.1"

)
