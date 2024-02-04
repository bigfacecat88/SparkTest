name :="example"
organization :="com.databricks"
//spark info
val sparkVersion="3.2.1"

//contains spark software
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at "http://repo.typesate.com/typesafe/simple/maven-releases/"

resolvers += "MavenRepository" at "https://mvnrepository.com/"


ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"



lazy val root = (project in file("."))
  .settings(
    name := "SparkTest"
)

libraryDependencies += "org.apache.parquet" %% "parquet-avro" % "1.13.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"



