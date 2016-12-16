import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Resume Downloaded
 * Created by Ivan on 2016/12/3.
 */
object FeatureExtractorResumeDownloaded {

  /*
    [Table] 58data_resume_download
   */
  def createTableResumeDownload(sc: SparkContext,
                          sqlContext: org.apache.spark.sql.SQLContext
                           ) = {

    val textFiles = sc.textFile("hdfs:///zp/58Data/resumedown/resumedown_*")

    val schemaString = "entid resumeuserid resumeid downtime "
    val dataStructure = new StructType(
        schemaString.split(" ").map(fieldName =>
          StructField(fieldName, StringType, nullable = false)
        )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 13)
      .map(xs => Row(xs(4), xs(7), xs(8), xs(12)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_resume_download")
  }

  def createTableEntPosition(sc: SparkContext,
                                sqlContext: org.apache.spark.sql.SQLContext
                                 ) = {

    val textFiles = sc.textFile("hdfs:///zp/58Data/enterprise_re_user/enterprise_re_user_*")

    val schemaString = "positionid userid entid "
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 3)
      .map(xs => Row(xs(0), xs(1), xs(2)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_ent_position")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    createTableResumeDownload(sc, sqlContext)
    createTableEntPosition(sc, sqlContext)

    val results = sqlContext.sql(
      """
        SELECT
          rd.resumeid,
          DATE_FORMAT(rd.downtime, 'YYYYMMdd') AS downloaddate,
          ep.positionid
        FROM 58data_resume_download rd
        JOIN 58data_ent_position ep
        ON
          rd.entid = ep.entid
          AND rd.resumeid <> '-'
          AND (
            DATE_FORMAT(rd.downtime, 'YYYYMMdd') LIKE '201609%'
            OR DATE_FORMAT(rd.downtime, 'YYYYMMdd') LIKE '201610%'
          )
      """.stripMargin)

    import sqlContext.implicits._
    results.repartition($"downloaddate").write.mode("overwrite").save("hdfs:///user/shuyangshi/58feature_resumedownloads")
  }

}
