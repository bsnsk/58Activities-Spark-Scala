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

    val labeledData = sc.textFile("hdfs:///user/shuyangshi/58data_labeledNoSQL/part-*")

    val validIds = labeledData
      .map(r => {
        val id = r.split(',')(0).split('[')(1)
        (id, 1)
      })
      .reduceByKey(_+_)
      .filter(_._2 > 3)

    validIds
      .join(tableDeliveryMatches.map(r => (r(0).toString, 1)))
      .join(tableDownloadMatches.map(r => (r(0).toString, 1)))
      .map(r => r._1)
      .toDF("resumeid")
      .registerTempTable("58data_actionresumes")

    val calcDist = (sa: Seq[Double], sb: Seq[Double]) => {
      (sa, sb).zipped.map((x, y) => (x-y)*(x-y)).sum
    }

    val dataRdd = sqlContext.sql(
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
        JOIN 58data_actionresumes rs
        ON t.resumeid = rs.resumeid
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

    val outputPath = "hdfs:///user/shuyangshi/58data_similarResumeData"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    dataRdd
      .saveAsTextFile(outputPath)

//    val results = dataRdd
//      .mapPartitions(dayDataIter => {
//        val dayData = dayDataIter.toArray.map(xs =>
//          (xs(1).toString, (xs(0).toString, xs.toSeq.drop(2).map(_.toString.toDouble)))
//        )
//        var results: List[(String, Array[String])] = List()
//        for (i <- dayData.indices) {
//          var minimumIs: Array[Int] = Array(-1, -1, -1)
//          var minimumVs: Array[Double] = Array(1e30, 1e30, 1e30)
//          for (j <- dayData.indices) {
//            if (i != j) {
//              val currentV = calcDist(dayData(i)._2._2, dayData(j)._2._2)
//              var updated: Boolean = false
//              var k:Int = 0
//              while (k < 3 && !updated) {
//                if (minimumIs(k) == -1 || currentV < minimumVs(k)) {
//                  minimumIs(k) = j
//                  minimumVs(k) = currentV
//                  updated = true
//                }
//              }
//            }
//          }
//          results = results.:+((dayData(i)._2._1, minimumIs.map(x => dayData(x)._2._1)))
//        }
//        results.iterator
//      }, true)
//
//    val outputPath = "hdfs:///user/shuyangshi/58data_similarResumes"
//    val fs = FileSystem.get(sc.hadoopConfiguration)
//    fs.delete(new Path(outputPath), true)
//    results
//      .saveAsTextFile(outputPath)
  }
}
