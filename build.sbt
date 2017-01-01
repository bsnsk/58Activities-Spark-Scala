name := "scalaspark"

version := "1.0"

scalaVersion := "2.10.5"

mainClass := Some("FeatureExtractorUserAction")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.1"
  , "org.apache.spark" %% "spark-sql" % "1.6.1"
  , "org.apache.spark" %% "spark-mllib" % "1.6.1"
  , "org.apache.spark" %% "spark-hive" % "1.6.1"
  )
