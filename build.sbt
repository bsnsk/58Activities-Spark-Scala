name := "scalaspark"

version := "1.0"

scalaVersion := "2.10.5"

mainClass := Some("FeatureExtractorUserAction")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1"
)
// libraryDependencies ++= Seq(
//     "org.apache.spark" % "spark-core_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")
//   , "org.apache.spark" % "spark-sql_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")
//   // , "org.apache.hadoop" % "
// Scala-Version: 2.10.5hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")
// )