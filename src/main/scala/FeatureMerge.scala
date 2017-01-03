import org.apache.spark.{SparkContext, SparkConf}

/**
 * Merge extracted features into a all-feature table
 * Created by Ivan on 2016/12/8.
 */
object FeatureMerge {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableUserAction = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userclicks/*.parquet")
    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")
    val tableResumeDownloaded  = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumedownloads/*.parquet")
    val tableTimeSeries = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_timeseries/*.parquet")
    val tableDeliveryMatches = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_deliverymatches/*.parquet")
    val tableDownloadMatches = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_downloadmatches/*.parquet")

    tableUserAction.registerTempTable("58data_userclicks")
    tableUserDelivery.registerTempTable("58data_userdeliveries")
    tableResumeDownloaded.registerTempTable("58data_resumedownloads")
    tableTimeSeries.registerTempTable("58data_timeseries")
    tableDeliveryMatches.registerTempTable("58data_deliverymatches")
    tableDownloadMatches.registerTempTable("58data_downloadmatches")

    val results = sqlContext.sql(
    """
      SELECT
        ids.resumeid AS resume_id,
        ids.date,
        COALESCE(
          SUM(uc.clickcount),
          0
        ) AS click_count,
        COALESCE(
          COUNT(ud.positionid),
          0
        ) AS delivery_count,
        COALESCE(
          COUNT(rd.positionid),
          0
        ) AS download_count,
        COALESCE(
          FIRST(ts.sum_clicks),
          0
        ) AS period_click_count,
        COALESCE(
          FIRST(ts.std_clicks),
          0
        ) AS period_click_std,
        COALESCE(
          FIRST(ts.sum_deliveries),
          0
        ) AS period_delivery_count,
        COALESCE(
          FIRST(ts.std_deliveries),
          0
        ) AS period_delivery_std,
        COALESCE(
          FIRST(ts.sum_downloads),
          0
        ) AS period_download_count,
        COALESCE(
          FIRST(ts.std_downloads),
          0
        ) AS period_download_std,
        COALESCE(
          FIRST(dv.avematch),
          0
        ) AS delivery_ave_match,
        COALESCE(
          FIRST(dv.maxmatch),
          0
        ) AS delivery_max_match,
        COALESCE(
          FIRST(dv.varmatch),
          0
        ) AS delivery_var_match,
        COALESCE(
          FIRST(dl.avematch),
          0
        ) AS download_ave_match

      FROM (
        SELECT DISTINCT
          t.resumeid,
          t.date
        FROM (
          SELECT
            resumeid,
            clickdate AS date
          FROM 58data_userclicks

          UNION ALL

          SELECT
            resumeid,
            deliverydate AS date
          FROM 58data_userdeliveries

          UNION ALL

          SELECT
            resumeid,
            downloaddate AS date
          FROM 58data_resumedownloads

          UNION ALL

          SELECT
            resumeid,
            date
          FROM 58data_timeseries

          UNION ALL

          SELECT
            resumeid,
            deliverydate AS date
          FROM 58data_deliverymatches

          UNION ALL

          SELECT
            resumeid,
            downloaddate AS date
          FROM 58data_downloadmatches

        ) t
      ) ids

      LEFT OUTER JOIN 58data_userclicks uc
      ON
        ids.resumeid = uc.resumeid
        AND ids.date = uc.clickdate

      LEFT OUTER JOIN 58data_userdeliveries ud
      ON
        ids.resumeid = ud.resumeid
        AND ids.date = ud.deliverydate

      LEFT OUTER JOIN 58data_resumedownloads rd
      ON
        ids.resumeid = rd.resumeid
        AND ids.date = rd.downloaddate

      LEFT OUTER JOIN 58data_timeseries ts
      ON
        ids.resumeid = ts.resumeid
        AND ids.date = ts.date

      LEFT OUTER JOIN 58data_deliverymatches dv
      ON
        ids.resumeid = dv.resumeid
        AND ids.date = dv.deliverydate

      LEFT OUTER JOIN 58data_downloadmatches dl
      ON
        ids.resumeid = dl.resumeid
        AND ids.date = dl.downloaddate

      GROUP BY ids.resumeid, ids.date
    """.stripMargin)

    results
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_all")

  }

  def mainOptimized(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableUserAction = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userclicks/*.parquet")
    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")
    val tableResumeDownloaded  = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumedownloads/*.parquet")
    val tableTimeSeries = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_timeseries/*.parquet")
    val tableDeliveryMatches = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_deliverymatches/*.parquet")
    val tableDownloadMatches = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_downloadmatches/*.parquet")

    tableUserAction.registerTempTable("58data_userclicks")
    tableUserDelivery.registerTempTable("58data_userdeliveries")
    tableResumeDownloaded.registerTempTable("58data_resumedownloads")
    tableTimeSeries.registerTempTable("58data_timeseries")
    tableDeliveryMatches.registerTempTable("58data_deliverymatches")
    tableDownloadMatches.registerTempTable("58data_downloadmatches")

    val dataIds = tableUserAction
      .map(xs => (xs(0), xs(1)))
      .union(
        tableUserDelivery
          .map(xs => (xs(1), xs(3)))
      )
      .union(
        tableResumeDownloaded
          .map(xs => (xs(0), xs(1)))
      )
      .union(
        tableTimeSeries
          .map(xs => (xs(0), xs(1)))
      )
      .union(
        tableDeliveryMatches
          .map(xs => (xs(0), xs(1)))
      )
      .union(
        tableDownloadMatches
          .map(xs => (xs(0), xs(1)))
      )
      .distinct()

    dataIds.toDF("resumeid", "date")
      .repartition($"date")
      .registerTempTable("validids")


    val results = sqlContext.sql(
      """
      SELECT
        ids.resumeid,
        ids.date,
        COALESCE(
          SUM(uc.clickcount),
          0
        ) AS clickcount,
        COALESCE(
          COUNT(ud.positionid),
          0
        ) AS deliverycount,
        COALESCE(
          COUNT(rd.positionid),
          0
        ) AS downloadcount,
        COALESCE(
          FIRST(ts.sumclicks),
          0
        ) AS periodclickcount,
        COALESCE(
          FIRST(ts.sumdeliveries),
          0
        ) AS perioddeliverycount,
        COALESCE(
          FIRST(ts.sumdownloads),
          0
        ) AS perioddownloadcount,
        COALESCE(
          FIRST(dv.avematch),
          0
        ) AS deliveryavematch,
        COALESCE(
          FIRST(dl.avematch),
          0
        ) AS downloadavematch

      FROM validids ids

      LEFT OUTER JOIN 58data_userclicks uc
      ON
        ids.resumeid = uc.resumeid
        AND ids.date = uc.clickdate

      LEFT OUTER JOIN 58data_userdeliveries ud
      ON
        ids.resumeid = ud.resumeid
        AND ids.date = ud.deliverydate

      LEFT OUTER JOIN 58data_resumedownloads rd
      ON
        ids.resumeid = rd.resumeid
        AND ids.date = rd.downloaddate

      LEFT OUTER JOIN 58data_timeseries ts
      ON
        ids.resumeid = ts.resumeid
        AND ids.date = ts.date

      LEFT OUTER JOIN 58data_deliverymatches dv
      ON
        ids.resumeid = dv.resumeid
        AND ids.date = dv.deliverydate

      LEFT OUTER JOIN 58data_downloadmatches dl
      ON
        ids.resumeid = dl.resumeid
        AND ids.date = dl.downloaddate

      GROUP BY ids.resumeid, ids.date
      """.stripMargin)

    results
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_all")

  }
}
