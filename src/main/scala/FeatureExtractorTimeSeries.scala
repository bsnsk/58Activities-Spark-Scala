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
        SELECT
          uc1.resumeid AS resumeid,
          uc1.clickdate AS date,
          SUM(uc1.clickcount)
            + SUM(uc2.clickcount)
            AS clicks3days
        FROM 58data_userclicks uc1
        LEFT JOIN 58data_userclicks uc2
        ON
          uc1.resumeid = uc2.resumeid
          AND (
            uc1.clickdate = uc2.clickdate + 1
            OR uc1.clickdate = uc2.clickdate + 2
          )
        GROUP BY uc1.resumeid, uc1.clickdate
      """.stripMargin)
    .registerTempTable("58temp_time_userclicks")

    sqlContext.sql(
      """
        SELECT
          ud1.resumeid AS resumeid,
          ud1.deliverydate AS date,
          COUNT(ud1.positionid)
            + COUNT(ud2.positionid)
            AS deliveries3days
        FROM 58data_userdeliveries ud1
        JOIN 58data_userdeliveries ud2
        ON
          ud1.resumeid = ud2.resumeid
          AND (
            ud1.deliverydate = ud2.deliverydate + 1
            OR ud1.deliverydate = ud2.deliverydate + 2
          )
        GROUP BY ud1.resumeid, ud1.deliverydate
      """.stripMargin)
      .registerTempTable("58temp_time_userdeliveries")

    sqlContext.sql(
    """
      SELECT
        rd1.resumeid AS resumeid,
        rd1.downloaddate AS date,
        COUNT(rd1.positionid)
          + COUNT(rd2.positionid)
          AS downloads3days
        FROM 58data_resumedownloads rd1
        JOIN 58data_resumedownloads rd2
        ON
          rd1.resumeid = rd2.resumeid
          AND (
            rd1.downloaddate = rd2.downloaddate + 1
            OR rd1.downloaddate = rd2.downloaddate + 2
          )
        GROUP BY rd1.resumeid, rd1.downloaddate
    """.stripMargin
    ).registerTempTable("58temp_time_resumedownloads")

    val timeSeriesTable = sqlContext.sql(
      """
        SELECT
          ids.resumeid,
          ids.date,
          COALESCE(
            uc.clicks3days,
            0
          ) AS sumclicks,
          COALESCE(
            ud.deliveries3days,
            0
          ) AS sumdeliveries
        FROM (
          SELECT DISTINCT
            resumeid,
            date
          FROM (
            SELECT
              resumeid,
              date
            FROM 58temp_time_userclicks

            UNION ALL

            SELECT
              resumeid,
              date
            FROM 58temp_time_userdeliveries

            UNION ALL

            SELECT
              resumeid,
              date
            FROM 58temp_time_resumedownloads
          )
        ) ids

        LEFT OUTER JOIN 58temp_time_userclicks uc
        ON
          ids.resumeid = uc.resumeid
          AND ids.date = uc.date

        LEFT OUTER JOIN 58temp_time_userdeliveries ud
        ON
          ids.resumeid = ud.resumeid
          AND ids.date = ud.date

        LEFT OUTER JOIN 58temp_time_resumedownloads rd
        ON
          ids.resumeid = rd.resumeid
          AND ids.date = rd.date
      """.stripMargin)

    timeSeriesTable
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_timeseries")
  }
}
