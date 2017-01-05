import breeze.numerics.abs
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Feature: Resume download matches
 * Created by Ivan on 2016/12/17.
 */
object FeatureExtractorDownloadMatches extends FeatureExtractorTemplateMatches {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tableResumeDownload = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumedownloads/*.parquet")

    val downloadByResume = tableResumeDownload
      .map(xs => (
        xs(0),
        ( xs(2),
          xs(1)
          )
        ))
      .distinct()

    val downloadMatches = calcMatchFeatureLists(downloadByResume, sqlContext)
    val downloadMatchesFull = calcMatchStatisticsFeatures(downloadMatches)

    val schemaString = "resumeid downloaddate " + featureString
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val namedDF = sqlContext.createDataFrame(
      downloadMatchesFull,
      dataStructure
    )

    import sqlContext.implicits._

    namedDF.repartition($"downloaddate").write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_downloadmatches")

  }

}
