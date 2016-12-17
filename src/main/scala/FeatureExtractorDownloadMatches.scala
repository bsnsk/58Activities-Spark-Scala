import breeze.numerics.abs
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Feature: Resume download matches
 * Created by Ivan on 2016/12/17.
 */
object FeatureExtractorDownloadMatches {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tableResumeDownload = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumedownloads/*.parquet")
    val tableResumeDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumes/*.parquet")
    val tablePositionDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_positions/*.parquet")

    val downloadByResume = tableResumeDownload
      .map(xs => (
        xs(0),
        ( xs(2),
          xs(1)
          )
        ))
      .distinct()

    val dataResumeDetail = tableResumeDetail
      .map(xs => (
        xs(0),
        ( xs(2),
          xs(3),
          xs(4),
          xs(5),
          xs(6),
          xs(7),
          xs(8),
          xs(9),
          xs(10),
          xs(11)
          )
        ))

    val dataPositionDetail = tablePositionDetail
      .map(xs => (
        xs(0),
        (
          xs(1),
          xs(2),
          xs(3),
          xs(4),
          xs(5),
          xs(6),
          xs(7),
          xs(8),
          xs(9)
          )
        ))

    val entryWeight = (x: Int, y: Int) => (10 - abs(x-y)).toDouble

    val downloadMatchesHalf = downloadByResume.join(dataResumeDetail)
      .map {
        case (resumeid, (
          (positionid, downloaddate),
          (nowposition, targetcategory, targetposition,
          targetsalary, education, gender,
          jobstate, areaid, complete, nowsalary)
          )) =>
          (positionid, (
            resumeid,
            downloaddate,
            nowposition,
            targetcategory,
            targetposition,
            targetsalary,
            education,
            gender,
            jobstate,
            areaid,
            complete,
            nowsalary
            ))
      }

    System.out.println("AFTERRESUMEJOIN: #" + downloadMatchesHalf.count().toString)

    val downloadMatches = downloadMatchesHalf
      .join(dataPositionDetail)
      .map {
        case (positionid,
        ((resumeid,
        rdownloaddate,
        rnowposition,
        rtargetcategory,
        rtargetposition,
        rtargetsalary,
        reducation,
        rgender,
        rjobstate,
        rareaid,
        rcomplete,
        rnowsalary),
        ( padddate,
        pcategory1,
        pcategory2,
        pcategory3,
        ptitle,
        psalary,
        peducation,
        pexperience,
        ptrade)

          )) =>
          ((resumeid, rdownloaddate), {
            calcStringFormula(rtargetsalary.toString, psalary.toString, entryWeight)
            + calcStringFormula(reducation.toString, peducation.toString, entryWeight)
            + calcStringFormula(rcomplete.toString, "", (x:Int, y:Int) => x / 10)
            + calcStringFormula(rtargetsalary.toString, rnowsalary.toString, entryWeight)
          })
      }

    val downloadCountMatches = downloadMatches.map(xs => (xs._1, 1)).reduceByKey(_+_)
    val downloadSumMatches = downloadMatches.reduceByKey(_+_)
    val downloadAveMatches = downloadSumMatches.join(downloadCountMatches)
      .map {
        case (key, (sumByKey, cnt))
        => (key, sumByKey / cnt)
      }
      .map(xs => (xs._1._1, xs._1._2, xs._2))

    val schemaString = "resumeid downloaddate avematch"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val rowRDD = downloadAveMatches
      .map(xs => Row(xs._1, xs._2, xs._3))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    import sqlContext.implicits._

    namedDF.repartition($"downloaddate").write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_downloadmatches")

  }

  def calcStringFormula(x: String, y: String, f: (Int, Int) => Double): Double = {
    try {
      if (x.length > 2 || y.length > 2) {
        return 0
      }
      val xInt = x.toInt
      val yInt = y.toInt
      f(xInt, yInt)
    } catch {
      case _: Throwable =>
        0
    }
  }
}
