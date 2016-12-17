import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Aggregate training data and labels together
 * Created by Ivan on 2016/12/17.
 */
object DataAggregator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableFeatures = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_all/*.parquet")
    val tableLabels = sqlContext.read.parquet("hdfs:///user/shuyangshi/58label_activeness/*.parquet")
    tableFeatures.registerTempTable("58data_features")
    tableLabels.registerTempTable("58data_labels")

    val labeledData = sqlContext.sql(
      """
        SELECT
          COALESCE(
            label.activeness,
            '0'
          ) AS activeness,
          feature.*
        FROM 58data_labels label
        FULL OUTER JOIN 58data_features feature
        ON
          label.resumeid = feature.resumeid
          AND label.date = feature.date + 1
      """.stripMargin)

    labeledData
      .repartition($"date")
      .rdd
      .saveAsTextFile("hdfs:///user/shuyangshi/58data_labeled")
//    labeledData.repartition($"date").write.mode("overwrite").save("hdfs:///user/shuyangshi/58data_labeled")
  }

}
