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
        | SUM(clickcount) AS daily_count
        |FROM 58data_userclicks
        |GROUP BY resumeid, clickdate
      """.stripMargin)
    .registerTempTable("58temp_daily_userclicks")

    val dataClicks = sqlContext.sql(
      """
         | SELECT
         |  uc_period.resumeid,
         |  uc_period.date,
         |  SUM(uc_period.daily_count) AS clicks_sum,
         |  STDDEV_POP(uc_period.daily_count) AS clicks_std,
         |  MAX(uc_period.daily_count) AS clicks_max
         | FROM (
         |  SELECT
         |    uc.resumeid,
         |    uc.date,
         |    uc.daily_count
         |  FROM 58temp_daily_userclicks uc
         |
         |  UNION ALL
         |
         |  SELECT
         |    uc.resumeid,
         |    DATE_FORMAT(
         |      DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(uc.date, 'yyyyMMdd')), -1),
         |      'yyyyMMdd'
         |    ) AS date,
         |    uc.daily_count
         |  FROM 58temp_daily_userclicks uc
         |
         |  UNION ALL
         |
         |  SELECT
         |    uc.resumeid,
         |    DATE_FORMAT(
         |      DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(uc.date, 'yyyyMMdd')), -2),
         |      'yyyyMMdd'
         |    ) AS date,
         |    uc.daily_count
         |  FROM 58temp_daily_userclicks uc
         | ) uc_period
         | GROUP BY uc_period.resumeid, uc_period.date
         | HAVING COUNT(uc_period.daily_count) = 3
      """.stripMargin)
    dataClicks.registerTempTable("58temp_time_userclicks")

    sqlContext.sql(
      """
        |SELECT
        | resumeid,
        | deliverydate AS date,
        | COUNT(positionid) AS daily_count
        |FROM 58data_userdeliveries
        |GROUP BY resumeid, deliverydate
      """.stripMargin)
    .registerTempTable("58temp_daily_userdeliveries")

    val dataDeliveries = sqlContext.sql(
      """
        | SELECT
        |  ud_period.resumeid,
        |  ud_period.date,
        |  SUM(ud_period.daily_count) AS deliveries_sum,
        |  STDDEV_POP(ud_period.daily_count) AS deliveries_std,
        |  MAX(ud_period.daily_count) AS deliveries_max
        | FROM (
        |  SELECT
        |    ud.resumeid,
        |    ud.date,
        |    ud.daily_count
        |  FROM 58temp_daily_userdeliveries ud
        |
        |  UNION ALL
        |
        |  SELECT
        |    ud.resumeid,
        |    DATE_FORMAT(
        |      DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(ud.date, 'yyyyMMdd')), -1),
        |      'yyyyMMdd'
        |    ) AS date,
        |    ud.daily_count
        |  FROM 58temp_daily_userdeliveries ud
        |
        |  UNION ALL
        |
        |  SELECT
        |    ud.resumeid,
        |    DATE_FORMAT(
        |      DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(ud.date, 'yyyyMMdd')), -2),
        |      'yyyyMMdd'
        |    ) AS date,
        |    ud.daily_count
        |  FROM 58temp_daily_userdeliveries ud
        | ) ud_period
        | GROUP BY ud_period.resumeid, ud_period.date
        | HAVING COUNT(ud_period.daily_count) = 3
      """.stripMargin)
    dataDeliveries.registerTempTable("58temp_time_userdeliveries")

    sqlContext.sql(
      """
        |SELECT
        | resumeid,
        | downloaddate AS date,
        | downcount AS daily_count
        |FROM 58data_resumedownloads
      """.stripMargin)
    .registerTempTable("58temp_daily_resumedownloads")

    val dataDownloads = sqlContext.sql(
      """
        | SELECT
        |  rd_period.resumeid,
        |  rd_period.date,
        |  SUM(rd_period.daily_count) AS downloads_sum,
        |  STDDEV_POP(rd_period.daily_count) AS downloads_std,
        |  MAX(rd_period.daily_count) AS downloads_max
        | FROM (
        |  SELECT
        |    rd.resumeid,
        |    rd.date,
        |    rd.daily_count
        |  FROM 58temp_daily_resumedownloads rd
        |
        |  UNION ALL
        |
        |  SELECT
        |    rd.resumeid,
        |    DATE_FORMAT(
        |      DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(rd.date, 'yyyyMMdd')), -1),
        |      'yyyyMMdd'
        |    ) AS date,
        |    rd.daily_count
        |  FROM 58temp_daily_resumedownloads rd
        |
        |  UNION ALL
        |
        |  SELECT
        |    rd.resumeid,
        |    DATE_FORMAT(
        |      DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(rd.date, 'yyyyMMdd')), -2),
        |      'yyyyMMdd'
        |    ) AS date,
        |    rd.daily_count
        |  FROM 58temp_daily_resumedownloads rd
        | ) rd_period
        | GROUP BY rd_period.resumeid, rd_period.date
        | HAVING COUNT(rd_period.daily_count) = 3
      """.stripMargin)
    dataDownloads.registerTempTable("58temp_time_resumedownloads")

    val dataAll = dataClicks
      .map(xs => ((xs(0), xs(1)), (xs(2), xs(3), xs(4))))
      .filter(xs => xs._1._1 != null && xs._1._2 != null && xs._2 != null)
      .fullOuterJoin(dataDeliveries
        .map(xs => ((xs(0), xs(1)), (xs(2), xs(3), xs(4))))
        .filter(xs => xs._1._1 != null && xs._1._2 != null && xs._2 != null)
      )
      .map {
        case (key, (numClicks, numDeliveries)) => (key, (
          if (numClicks.isDefined) numClicks.get._1.toString else "0",
          if (numClicks.isDefined) numClicks.get._2.toString else "0",
          if (numClicks.isDefined) numClicks.get._3.toString else "0",
          if (numDeliveries.isDefined) numDeliveries.get._1.toString else "0",
          if (numDeliveries.isDefined) numDeliveries.get._2.toString else "0",
          if (numDeliveries.isDefined) numDeliveries.get._3.toString else "0"
        ))
      }
      .fullOuterJoin(dataDownloads
        .map(xs => ((xs(0), xs(1)), (xs(2), xs(3), xs(4))))
      )
      .map {
        case (key, (exist, numDownloads)) => Row(
          key._1,
          key._2,
          if (exist.isDefined) exist.get._1 else "0",
          if (exist.isDefined) exist.get._2 else "0",
          if (exist.isDefined) exist.get._3 else "0",
          if (exist.isDefined) exist.get._4 else "0",
          if (exist.isDefined) exist.get._5 else "0",
          if (exist.isDefined) exist.get._6 else "0",
          if (numDownloads.isDefined) numDownloads.get._1.toString else "0",
          if (numDownloads.isDefined) numDownloads.get._2.toString else "0",
          if (numDownloads.isDefined) numDownloads.get._3.toString else "0"
        )
      }

    val schemaString = "resumeid date " +
      "sum_clicks std_clicks max_clicks " +
      "sum_deliveries std_deliveries max_deliveries " +
      "sum_downloads std_downloads max_downloads"
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
