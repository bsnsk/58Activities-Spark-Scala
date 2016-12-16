import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Feature: Resume id - Date - ClickTag - ClickCount
 * Created by Ivan on 2016/12/3.
 */
object FeatureExtractorUserAction {

  def createTableUserAction(sc: SparkContext,
                  sqlContext: org.apache.spark.sql.SQLContext) {
    val textFiles = sc.textFile("hdfs:///zp/58Data/useraction/useraction_*")

    val schemaString = "cookieid clicktag clicktime userid infoid"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 5)
      .map(xs => Row(xs(0), xs(1), xs(2), xs(3), xs(4)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_useractions")
  }

  def createTableUserResume(sc: SparkContext,
                            sqlContext: org.apache.spark.sql.SQLContext) {
    val textFiles = sc.textFile("hdfs:///zp/58Data/resume/resume_*")

    val schemaString = "resumeid userid adddate"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 20)
      .map(xs => Row(xs(0), xs(1), xs(19)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_user_resume")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    createTableUserAction(sc, sqlContext)
    createTableUserResume(sc, sqlContext)

    val results = sqlContext.sql(
      """
        SELECT
          r.resumeid AS resumeid,
          FROM_UNIXTIME(SUBSTR(a.clicktime, 1, 10),'YYYYMMdd')
            AS clickdate,
          a.clicktag AS clicktag,
          COUNT(a.infoid) AS clickcount
        FROM 58data_useractions a
        JOIN 58data_user_resume r
        WHERE
          a.userid <> '-'
          AND r.userid = a.userid
          AND a.clicktime >= r.adddate
          AND (
            FROM_UNIXTIME(SUBSTR(a.clicktime, 1, 10), 'YYYYMMdd')
              LIKE '201609%'
            OR FROM_UNIXTIME(SUBSTR(a.clicktime, 1, 10), 'YYYYMMdd')
              LIKE '201610%'
          )
        GROUP BY
          r.resumeid,
          a.clicktag,
          FROM_UNIXTIME(SUBSTR(a.clicktime, 1, 10), 'YYYYMMdd')
      """.stripMargin)

    import sqlContext.implicits._
    results.repartition($"clickdate").write.mode("overwrite").save("hdfs:///user/shuyangshi/58feature_userclicks")
  }

}
