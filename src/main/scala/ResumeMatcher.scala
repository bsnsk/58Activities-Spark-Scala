import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 2017/3/9.
  */
object ResumeMatcher {

  def formatMatchesFeatureSelects(tableAlias: String, actionName: String): String = {
    val featureList = new FeatureExtractorTemplateMatches().featureString.split(" ")
    featureList.map(name =>
      s"""
        COALESCE(
          FIRST($tableAlias.$name),
          0
        ) AS ${actionName}_$name"""
        .stripMargin)
      .mkString(",\n")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureMerger")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    import scala.util.control.Breaks._

    val tableDeliveryMatches = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_deliverymatches/*.parquet")
    val tableDownloadMatches = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_downloadmatches/*.parquet")

    tableDeliveryMatches.registerTempTable("58data_deliverymatches")
    tableDownloadMatches.registerTempTable("58data_downloadmatches")

    val calcDist = (sa: Seq[Double], sb: Seq[Double]) => {
      (sa, sb).zipped.map((x, y) => (x-y)*(x-y)).sum
    }

    val results = sqlContext.sql(
      s"""
      SELECT
        ids.resumeid AS resume_id,
        ids.date,

        ${formatMatchesFeatureSelects("dv", "delivery")},

        ${formatMatchesFeatureSelects("dl", "download")}

      FROM (
        SELECT DISTINCT
          t.resumeid,
          t.date
        FROM (

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
      .repartition($"date")
      .rdd
      .map(xs => (xs(1).toString, (xs(0).toString, xs.toSeq.drop(2).map(_.toString.toDouble))))
      .groupByKey()
      .persist(StorageLevel.DISK_ONLY)
      .map(dayData => {
        val resData = dayData._2.toArray
        var results: List[(String, Array[String])] = List()
        for (i <- resData.indices) {
          var minimumIs: Array[Int] = Array(-1, -1, -1)
          var minimumVs: Array[Double] = Array(1e30, 1e30, 1e30)
          for (j <- resData.indices) {
            if (i != j) {
              val currentV = calcDist(resData(i)._2, resData(j)._2)
              var updated: Boolean = false
              var k:Int = 0
              while (k < 3 && !updated) {
                if (minimumIs(k) == -1 || currentV < minimumVs(k)) {
                  minimumIs(k) = j
                  minimumVs(k) = currentV
                  updated = true
                }
              }
            }
          }
          results = results.:+((resData(i)._1, minimumIs.map(x => resData(x)._1)))

        }
        (dayData._1, results)
      })
      .flatMap(xs => xs._2.map(r => ((r._1, xs._1), r._2)))

    val outputPath = "hdfs:///user/shuyangshi/58data_similarResumes"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    results
      .saveAsTextFile(outputPath)
  }
}
