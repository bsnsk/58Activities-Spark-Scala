import breeze.numerics.abs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Feature: Resume delivery matches
 * Created by Ivan on 2016/12/14.
 */
object FeatureExtractorDeliveryMatches extends FeatureExtractorTemplateMatches {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")

    val deliveryByResume = tableUserDelivery
      .map(xs => (
        xs(1),
        ( xs(2),
          xs(3)
        )
      ))
      .distinct()

    val deliveryMatches = calcMatchFeatureLists(deliveryByResume, sqlContext)
    val deliveryMatchesFull = calcMatchStatisticsFeatures(deliveryMatches)

    val schemaString = "resumeid deliverydate " + featureString
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val namedDF = sqlContext.createDataFrame(
      deliveryMatchesFull,
      dataStructure
    )

    import sqlContext.implicits._

    namedDF.repartition($"deliverydate").write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_deliverymatches")

  }

}
