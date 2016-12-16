import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Time series features, like
 *  average / standard deviation of # of active days in a while,
 *  average / standard deviation of # of clicks in a while,
 *  average / standard deviation of # of downloads in a while.
 *
 * Created by Ivan on 2016/12/11.
 */
object FeatureExtractorTimeSeries {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableUserAction = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userclicks/*.parquet")
    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")
    val tableResumeDownloaded  = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumedownloads/*.parquet")

    tableUserAction.registerTempTable("58data_userclicks")
    tableUserDelivery.registerTempTable("58data_userdeliveries")
    tableResumeDownloaded.registerTempTable("58data_resumedownloads")

    sqlContext.sql(
      """
        |SELECT
        | resumeid AS resumeid,
        | clickdate AS date,
        | SUM(clickcount) AS dailycount
        |FROM 58data_userclicks
        |GROUP BY resumeid, clickdate
      """.stripMargin)
    .registerTempTable("58temp_daily_userclicks")

    val dataClicks = sqlContext.sql(
      """
        SELECT
          uc1.resumeid AS resumeid,
          uc1.date AS date,
          FIRST(uc1.dailycount) + SUM(uc2.dailycount)
            AS clicks3days
        FROM 58temp_daily_userclicks uc1
        LEFT JOIN 58temp_daily_userclicks uc2
        ON
          uc1.resumeid = uc2.resumeid
          AND (
            uc1.date = uc2.date + 1
            OR uc1.date = uc2.date + 2
          )
        GROUP BY uc1.resumeid, uc1.date
      """.stripMargin)
    dataClicks.registerTempTable("58temp_time_userclicks")

    sqlContext.sql(
      """
        |SELECT
        | resumeid,
        | deliverydate AS date,
        | COUNT(positionid) AS dailycount
        |FROM 58data_userdeliveries
        |GROUP BY resumeid, deliverydate
      """.stripMargin)
    .registerTempTable("58temp_daily_userdeliveries")

    val dataDeliveries = sqlContext.sql(
      """
        SELECT
          ud1.resumeid AS resumeid,
          ud1.date AS date,
          FIRST(ud1.dailycount) + SUM(ud2.dailycount)
            AS deliveries3days
        FROM 58temp_daily_userdeliveries ud1
        JOIN 58temp_daily_userdeliveries ud2
        ON
          ud1.resumeid = ud2.resumeid
          AND (
            ud1.date = ud2.date + 1
            OR ud1.date = ud2.date + 2
          )
        GROUP BY ud1.resumeid, ud1.date
      """.stripMargin)
    dataDeliveries.registerTempTable("58temp_time_userdeliveries")

    sqlContext.sql(
      """
        |SELECT
        | resumeid,
        | downloaddate AS date,
        | COUNT(positionid) AS dailycount
        |FROM 58data_resumedownloads
        |GROUP BY resumeid, downloaddate
      """.stripMargin)
    .registerTempTable("58temp_daily_resumedownloads")

    val dataDownloads = sqlContext.sql(
    """
      SELECT
        rd1.resumeid AS resumeid,
        rd1.date AS date,
        FIRST(rd1.dailycount) + SUM(rd2.dailycount)
          AS downloads3days
        FROM 58temp_daily_resumedownloads rd1
        JOIN 58temp_daily_resumedownloads rd2
        ON
          rd1.resumeid = rd2.resumeid
          AND (
            rd1.date = rd2.date + 1
            OR rd1.date = rd2.date + 2
          )
        GROUP BY rd1.resumeid, rd1.date
    """.stripMargin)
    dataDownloads.registerTempTable("58temp_time_resumedownloads")

    val dataAll = dataClicks
      .map(xs => ((xs(0), xs(1)), xs(2)))
      .filter(xs => xs._1._1 != null && xs._1._2 != null && xs._2 != null)
      .fullOuterJoin(dataDeliveries
        .map(xs => ((xs(0), xs(1)), xs(2)))
        .filter(xs => xs._1._1 != null && xs._1._2 != null && xs._2 != null)
      )
      .map {
        case (key, (None, numDeliveries)) => (key, ("0", numDeliveries.get.toString))
        case (key, (numClicks, None)) => (key, (numClicks.get.toString, "0"))
        case (key, (numClicks, numDeliveries)) => (key, (numClicks.get.toString, numDeliveries.get.toString))
      }
      .fullOuterJoin(dataDownloads
        .map(xs => ((xs(0), xs(1)), xs(2)))
      )
      .map {
        case (key, (exist, None)) => Row(key._1, key._2, exist.get._1, exist.get._2, "0")
        case (key, (None, numDownloads)) => Row(key._1, key._2, "0", "0", numDownloads.get.toString)
        case (key, (exist, numDownloads)) => Row(key._1, key._2, exist.get._1, exist.get._2, numDownloads.get.toString)
      }

    val schemaString = "resumeid date sumclicks sumdeliveries sumdownloads"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    sqlContext
      .createDataFrame(
        dataAll,
        dataStructure
      )
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_timeseries")
  }
}
