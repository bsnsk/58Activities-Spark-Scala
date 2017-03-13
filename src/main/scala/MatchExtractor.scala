import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 2017/3/6.
  */
object MatchExtractor {

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
    val conf = new SparkConf().setAppName("SparkMatchExtractor")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val dTextFiles = sc.textFile("hdfs:///zp/58Data/delivery/delivery_*")
    val userdeliveries = dTextFiles.map(_.split("\001")).filter(_.length>=7).map(
      xs => (xs(3), xs(1), xs(5))
    ).filter(xs => xs._1 != "-" && xs._2 != "-")

    createTableUserAction(sc, sqlContext)
    createTableUserResume(sc, sqlContext)

    val userclicks = sqlContext.sql(
      """
        | SELECT
        |   r.resumeid,
        |   u.infoid,
        |   u.clicktime
        | FROM 58data_user_resume r
        |   JOIN 58data_useractions u
        |   WHERE
        |     r.userid = u.userid
      """.stripMargin
    ).rdd.map(xs => (xs(0), xs(1), xs(2)))

    val result = userclicks.map(x => (x._1, x._2, x._3, "C")).union(userdeliveries.map(x => (x._1, x._2, x._3, "D")))

    val filtered = result.map(
      xs => try {
        (xs._1.toString.toLong.toString, xs._2.toString.toLong.toString, xs._3.toString.toLong.toString, xs._4)
      } catch {
        case _:Throwable => ("-", "-", "-", "-")
      }
    ).filter(xs => xs._1 != "-" && xs._2 != "-" && xs._3 != "-")

    val outputPath = "hdfs:///user/shuyangshi/58data_resumeClickDeliveryPairs"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    filtered
      .repartition(1)
      .saveAsTextFile(outputPath)
  }


}
