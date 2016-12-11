import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Position details for matching with resumes
 * Created by Ivan on 2016/12/8.
 */
object FeatureExtractorPositionDetail {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    createTablePosition(sc, sqlContext)

    val results = sqlContext.sql(
      """
        SELECT *
        FROM 58data_positions
        WHERE
          infoid <> '-'
      """.stripMargin)

    results
      .repartition(1)
      .write
      .mode("overwrite")
      .save("hdfs:///user/shuyangshi/58data_positions")
  }

  def createTablePosition(sc: SparkContext,
                        sqlContext: org.apache.spark.sql.SQLContext
                         ) = {

    val textFiles = sc.textFile("hdfs:///zp/58Data/position/position_*")

    val schemaString = "infoid addate cate1 cate2 cate3 " +
      "title salary education experience trade"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 18)
      .map(xs => Row(xs(0), xs(1), xs(2), xs(3), xs(4),
        xs(6), xs(14), xs(15), xs(16), xs(17)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_positions")
  }
}
