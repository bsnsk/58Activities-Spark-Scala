import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StringType, StructType}
import org.apache.spark.{SparkContext, SparkConf}

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

    val allActive = clickActive.union(deliveryActive).distinct()

    /* Version 1: Full (Both active and inactive are included) */
//    val results = idx
//      .leftOuterJoin(allActive)
//      .map {
//        case (key, (inactive, None)) =>
//          (key._1, key._2, inactive)
//        case (key, (inactive, active)) =>
//          (key._1, key._2, active.get)
//      }
//      .map(xs => Row(xs._1, xs._2, xs._3))

    /* Version 2: Single status (Active only) */
    val results = allActive.map(xs => Row(xs._1._1, xs._1._2, xs._2.toString))

    val schemaString = "resumeid date activeness"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val resultsDF = sqlContext.createDataFrame(
      results,
      dataStructure
    )

    resultsDF
      .repartition($"date")
      .write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58label_activeness")
  }

  def mainWithSQL(args: Array[String]) {
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
