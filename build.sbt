name := "pig-udf"

version := "1.0"

crossPaths := false

autoScalaLibrary := false

libraryDependencies += "org.apache.pig" % "pig" % "0.9.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.0.1"

libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.5"

//javacOptions ++= Seq("-Xlint")
