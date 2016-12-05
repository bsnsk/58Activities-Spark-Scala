import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Ivan on 2016/12/3.
 */
object FeatureExtractorUserAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val textFiles = sc.textFile("hdfs:///zp/58Data/useraction/useraction_*")

    val dataStructure = new StructType(
      Array(
        StructField("cookieid", StringType, nullable = false),
        StructField("clicktag", StringType, nullable = false),
        StructField("clicktime", StringType, nullable = false),
        StructField("userid", StringType, nullable = true),
        StructField("infoid", StringType, nullable = false)
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

    val results = sqlContext.sql(
      """
        SELECT
          userid AS userid,
          FROM_UNIXTIME(clicktime,'YYYYMMdd') AS clickdate,
          clicktag AS clicktag,
          COUNT(infoid) AS clickcount
        FROM 58data_useractions
        WHERE
          userid <> '-'
        GROUP BY userid, clicktag, FROM_UNIXTIME(clicktime, 'YYYYMMdd')
      """.stripMargin)

//    results.rdd.saveAsTextFile("hdfs:///user/shuyangshi/58feature_userclicks")
    results.write.mode("overwrite").save("hdfs:///user/shuyangshi/58feature_userclicks")
  }

}
