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
//    val tableResumeDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumes/*.parquet")

    tableUserAction.registerTempTable("58data_userclicks")
    tableUserDelivery.registerTempTable("58data_userdeliveries")
    tableResumeDownloaded.registerTempTable("58data_resumedownloads")

    //    tableResumeDetail.registerTempTable("58data_resumes")

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
        ) AS downloadcount
      FROM (
        SELECT DISTINCT
          resumeid,
          date
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
        )
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

      GROUP BY ids.resumeid, ids.date
    """.stripMargin)

    results
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_all")

  }
}
