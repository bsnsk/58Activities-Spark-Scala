import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Feature: User clicks
 * Created by Ivan on 2016/12/3.
 */
object FeatureExtractorUserDelivery {

  /*
    [Table] 58data_delivery
   */
  def createTableDelivery(sc: SparkContext,
                          sqlContext: org.apache.spark.sql.SQLContext
                           ) = {

    val textFiles = sc.textFile("hdfs:///zp/58Data/delivery/delivery_*")

    val schemaString = "cookieid infoid entuserid resumeid resumeuserid deliverytime slot"
    val dataStructure = new StructType(
        schemaString.split(" ").map(fieldName =>
          StructField(fieldName, StringType, nullable = false)
        )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 7)
      .map(xs => Row(xs(0), xs(1), xs(2), xs(3), xs(4), xs(5), xs(6)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_delivery")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    createTableDelivery(sc, sqlContext)

    val results = sqlContext.sql(
      """
        SELECT
          resumeuserid AS userid,
          resumeid,
          infoid AS positionid,
          FROM_UNIXTIME(SUBSTR(deliverytime, 1, 10), 'YYYYMMdd') AS deliverydate
        FROM 58data_delivery
        WHERE resumeuserid <> '-'
        AND (
          FROM_UNIXTIME(SUBSTR(deliverytime, 1, 10), 'YYYYMMdd')
            LIKE '201609%'
          OR FROM_UNIXTIME(SUBSTR(deliverytime, 1, 10), 'YYYYMMdd')
            LIKE '201610%'
        )
      """.stripMargin)

    import sqlContext.implicits._
    results.repartition($"deliverydate").write.mode("overwrite").save("hdfs:///user/shuyangshi/58feature_userdeliveries")
  }

}
