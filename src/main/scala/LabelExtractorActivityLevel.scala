import org.apache.spark.{SparkContext, SparkConf}

/**
 * Label Extractor: Activity Level (0 or 1)
 * Created by Ivan on 2016/12/12.
 */
object LabelExtractorActivityLevel {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableUserAction = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userclicks/*.parquet")
    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")

    tableUserAction.registerTempTable("58data_userclicks")
    tableUserDelivery.registerTempTable("58data_userdeliveries")

    val clickThreshold = 3

    val results=sqlContext.sql(
      s"""
        SELECT DISTINCT
          resumes.resumeid,
          dates.date,
          CASE
            WHEN actives.activeness = 1 THEN 1
            ELSE 0
          END AS activeness
        FROM (
          SELECT DISTINCT clickdate AS date
          FROM 58data_userclicks
          UNION
          SELECT DISTINCT deliverydate AS date
          FROM 58data_userdeliveries
        ) dates
        LEFT OUTER JOIN (
          SELECT DISTINCT resumeid
          FROM 58data_userclicks
          UNION
          SELECT DISTINCT resumeid
          FROM 58data_userdeliveries
        ) resumes
        LEFT OUTER JOIN
        (
          SELECT
            resumeid,
            clickdate AS date,
            1 AS activeness
          FROM 58data_userclicks
          GROUP BY resumeid, clickdate
          HAVING SUM(clickcount) >= $clickThreshold

          UNION ALL

          SELECT
            resumeid,
            deliverydate AS date,
            1 AS activeness
          FROM 58data_userdeliveries
          GROUP BY resumeid, deliverydate
        ) actives
        ON
          resumes.resumeid = actives.resumeid
          AND dates.date = actives.date
      """.stripMargin)

    results
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58label_activeness")
  }

}
