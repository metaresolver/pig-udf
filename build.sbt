import AssemblyKeys._

name := "pig-udf"

version := "1.0"

jarName in assembly := "pig-udf-1.0.jar"

seq(assemblySettings: _*)

test in assembly := {} // turn off tests for assembly

mainClass in assembly := Some("com.meta.detect.Main")

crossPaths := false

autoScalaLibrary := false

libraryDependencies += "org.apache.pig" % "pig" % "0.9.2" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.0.1" % "provided"

libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.7.3"

//javacOptions ++= Seq("-Xlint")
