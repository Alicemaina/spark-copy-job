name := "Spark-Copy-Job"

version := "1.0"

scalaVersion := "2.10.6"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2" % "provided"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0" % "provided"