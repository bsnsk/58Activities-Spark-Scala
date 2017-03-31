import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Label Extractor: Activity Level (0 or 1)
 * Created by Ivan on 2016/12/12.
 */
object LabelExtractorActivityLevel {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkLabelExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableUserAction = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userclicks/*.parquet")
    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")

    val clickThreshold = 3

    val resumes = tableUserAction
      .map(row => Tuple1(row(0)))
      .distinct()
      .union(tableUserDelivery
        .map(row => Tuple1(row(1)))
        .distinct()
      )
      .distinct()

    val dates = tableUserAction
      .map(row => Tuple1(row(1)))
      .distinct()
      .union(tableUserDelivery
        .map(row => Tuple1(row(3)))
        .distinct()
      )
      .distinct()

    val idx = resumes.cartesian(dates).map(xs => ((xs._1._1, xs._2._1), 0))

    val clickActive = tableUserAction
      .map(xs => ((xs(0), xs(1)), 1))
      .reduceByKey(_+_)
      .filter(xs => xs._2 >= clickThreshold)
      .map(xs => ((xs._1._1, xs._1._2), 1))

    val deliveryActive = tableUserDelivery
      .map(xs => ((xs(1), xs(3)), 1))

    val allDigits: String => Boolean = string => {
      string.map(ch => Character.isDigit(ch) || ch.toString == "-" || ch.toString == ".").reduce(_ && _)
    }

    var historyClicks = resumes.map(x => (x._1.toString, Array():Array[Double]))
    var historyDeliveries = resumes.map(x => (x._1.toString, Array():Array[Double]))
    val datesArray = dates.map(_._1.toString)
      .filter(allDigits(_)).filter(_.toLong >= 20160101).filter(_.toLong <= 20180101)
      .collect().sorted
    val clickCounts = tableUserAction
      .map(xs => ((xs(0), xs(1)), 1))
      .reduceByKey(_+_)
      .map(xs => (xs._1._1.toString, (xs._1._2, xs._2)))
    val deliveryCounts = tableUserDelivery
      .map(xs => ((xs(1), xs(3)), 1))
      .reduceByKey(_+_)
      .map(xs => (xs._1._1.toString, (xs._1._2, xs._2)))

    for (date <- datesArray) {
      val clicksToday = clickCounts.filter(_._2._1.toString == date)
      val deliveryToday = deliveryCounts.filter(_._2._1.toString == date)
      historyClicks = historyClicks.leftOuterJoin(clicksToday).map {
        case (resumeid, (previous, newcomer)) => {
          if (newcomer.isDefined) {
            (resumeid, previous.:+(newcomer.get._2.toDouble))
          }
          else {
            (resumeid, previous.:+(0.0))
          }
        }
      }
      historyDeliveries = historyDeliveries.leftOuterJoin(deliveryToday).map {
        case (resumeid, (previous, newcomer)) => {
          if (newcomer.isDefined) {
            (resumeid, previous.:+(newcomer.get._2.toDouble))
          }
          else {
            (resumeid, previous.:+(0.0))
          }
        }
      }
    }
    val historyActivities = historyClicks.join(historyDeliveries)
    val filteredHistoryActivities = historyActivities
//      .filter(xs => {
//        var cnt = 0
//        for (i <- xs._2._1.indices) {
//          if (xs._2._1(i) > 3 || xs._2._2(i) > 0) {
//            cnt += 1
//          }
//        }
//        cnt > 3
//      })

    val outputPath = "hdfs:///user/shuyangshi/58feature_history"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    filteredHistoryActivities
      .repartition(1)
      .map(xs => (xs._1, (xs._2._1.toSeq, xs._2._2.toSeq)))
      .saveAsTextFile(outputPath)

//    val allActive = clickActive.union(deliveryActive).distinct()
//
//    /* Version 2: Single status (Active only) */
//    val results = allActive.map(xs => Row(xs._1._1, xs._1._2, xs._2.toString))
//
//    val schemaString = "resumeid date activeness"
//    val dataStructure = new StructType(
//      schemaString.split(" ").map(fieldName =>
//        StructField(fieldName, StringType, nullable = false)
//      )
//    )
//
//    val resultsDF = sqlContext.createDataFrame(
//      results,
//      dataStructure
//    )
//
//    resultsDF
//      .repartition($"date")
//      .write.mode("overwrite")
//      .save("hdfs:///user/shuyangshi/58label_activeness")
  }


}
