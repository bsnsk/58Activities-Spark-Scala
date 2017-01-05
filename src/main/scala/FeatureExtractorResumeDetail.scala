import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Resume Detail Feature
 * Created by Ivan on 2016/12/3.
 */
object FeatureExtractorResumeDetail {

  /*
    [Table] 58data_resume
   */
  def createTableResume(sc: SparkContext,
                          sqlContext: org.apache.spark.sql.SQLContext
                           ) = {

    val textFiles = sc.textFile("hdfs:///zp/58Data/resume/resume_20161*," +
      "hdfs:///zp/58Data/resume/resume_2016091*," +
      "hdfs:///zp/58Data/resume/resume_2016092[0-4]," +
      "hdfs:///zp/58Data/resume/resume_2016092[6-9]," +
      "hdfs:///zp/58Data/resume/resume_20160930")

    val schemaString = "resumeid userid nowposition targetcategory targetposition " +
      "targetsalary education gender jobstate areaid " +
      "complete nowsalary "
    val dataStructure = new StructType(
        schemaString.split(" ").map(fieldName =>
          StructField(fieldName, StringType, nullable = false)
        )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length == 34)
      .map(xs => Row(xs(0), xs(1), xs(2), xs(3), xs(4),
        xs(5), xs(6), xs(11), xs(13), xs(16),
        xs(17), xs(20)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_resume")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    createTableResume(sc, sqlContext)

    val results = sqlContext.sql(
      """
         SELECT *
         FROM 58data_resume
         WHERE resumeid <> '-'
      """.stripMargin)

    results.repartition(1).write.mode("overwrite").save("hdfs:///user/shuyangshi/58feature_resumes")
  }

}
