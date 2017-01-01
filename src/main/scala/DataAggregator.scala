import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
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

    val labeledDataNoSQL = tableFeatures
      .map(xs => ((xs(0), xs(1)), xs.toSeq.drop(2)))
      .leftOuterJoin(
        sqlContext.sql(
          """
            |SELECT
            | resumeid,
            | DATE_FORMAT(
            |   DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'yyyyMMdd')), -1),
            |   'yyyyMMdd'
            | ) AS date,
            | activeness
            |FROM 58data_labels
          """.stripMargin)
          .repartition($"date")
          .map(xs => ((xs(0), xs(1)), xs(2)))
      )
      .map{
        case (key, (features, None)) => (key._2.toString,  "0" :: features.map(_.toString).toList)
        case (key, (features, label)) => (key._2.toString,  "1" :: features.map(_.toString).toList)
                                                                            // **WARNING**: Labels fetched
                                                                            // previously contain only active ones
      }
      .toDF()

    labeledDataNoSQL
      .repartition($"_1")
      .rdd
      .saveAsTextFile("hdfs:///user/shuyangshi/58data_labeledNoSQL")
  }

  def mainSQL(): Unit ={
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableFeatures = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_all/*.parquet")
    val tableLabels = sqlContext.read.parquet("hdfs:///user/shuyangshi/58label_activeness/*.parquet")
    tableFeatures.registerTempTable("58data_features")
    tableLabels.registerTempTable("58data_labels")

    val labeledDataSQL = sqlContext.sql(
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
          AND label.date = DATE_FORMAT(
            DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(feature.date, 'yyyyMMdd')), 1),
            'yyyyMMdd'
          )
      """.stripMargin)

    labeledDataSQL
      .repartition($"date")
      .rdd
      .saveAsTextFile("hdfs:///user/shuyangshi/58data_labeledSQL")
  }

}
