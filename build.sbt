name := "hadoop-best-rate"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.0.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.0.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.0.3" % "provided"



libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0" % "test"
