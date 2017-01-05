import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Aggregate training data and labels together
 * Created by Ivan on 2016/12/17.
 */
object DataAggregator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkDataAggregator")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableFeatures = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_all/*.parquet")
    val tableLabels = sqlContext.read.parquet("hdfs:///user/shuyangshi/58label_activeness/*.parquet")

    val labeledDataNoSQL = tableFeatures
      .map(xs => ((xs(0), xs(1).toString), xs.toSeq.drop(2)))
      .leftOuterJoin(
        tableLabels
          .flatMap(xs => {
            val dateString = xs(1).toString
            val sdf = new SimpleDateFormat("yyyyMMdd")
            val date = Calendar.getInstance()
            date.setTime(sdf.parse(dateString))

            Array(1, 2, 3).map(n => {
              date.add(Calendar.DAY_OF_MONTH, -1)
              ((xs(0), sdf.format(date.getTime)), xs(2))
            })
          })
          .distinct()
          .flatMap(xs => List.fill(32)(xs)) // __TODO__ Balance negative / positive training samples
      )
      .map{
        case (key, (features, None)) => (key._2.toString,  0.toDouble :: features.map(_.toString.toDouble).toList)
        case (key, (features, label)) => (key._2.toString,  1.toDouble :: features.map(_.toString.toDouble).toList)
                                                                            // __WARNING__: Labels fetched
                                                                            // previously contain only active ones
      }
      .toDF()

    val outputPath = "hdfs:///user/shuyangshi/58data_labeledNoSQL"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    labeledDataNoSQL
      .repartition($"_1")
      .rdd
      .saveAsTextFile(outputPath)
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
