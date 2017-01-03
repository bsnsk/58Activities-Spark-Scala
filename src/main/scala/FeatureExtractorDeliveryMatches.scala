import breeze.numerics.abs
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Feature: Resume delivery matches
 * Created by Ivan on 2016/12/14.
 */
object FeatureExtractorDeliveryMatches {

  def calcStringFormula(x: String, y: String, f: (Int, Int) => Double): Double = {
    try {
      if (x.length > 2 || y.length > 2) {
        return 0.toDouble
      }
      val xInt = x.toInt
      val yInt = y.toInt
      f(xInt, yInt)
    } catch {
      case _: Throwable =>
        0.toDouble
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tableUserDelivery = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_userdeliveries/*.parquet")
    val tableResumeDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumes/*.parquet")
    val tablePositionDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_positions/*.parquet")

    val deliveryByResume = tableUserDelivery
      .map(xs => (
        xs(1),
        ( xs(2),
          xs(3)
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

    val deliveryMatches = deliveryByResume.join(dataResumeDetail)
      .map {
        case (resumeid, (
          (positionid, deliverydate),
          (nowposition, targetcategory, targetposition,
            targetsalary, education, gender,
            jobstate, areaid, complete, nowsalary)
          )) =>
          (positionid, (
            resumeid,
            deliverydate,
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
    .join(dataPositionDetail)
    .map {
      case (positionid,
        ((resumeid,
          rdeliverydate,
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
        ((resumeid, rdeliverydate), {
          calcStringFormula(rtargetsalary.toString, psalary.toString, entryWeight)
          + calcStringFormula(reducation.toString, peducation.toString, entryWeight)
          + calcStringFormula(rcomplete.toString, "", (x:Int, y:Int) => x.toDouble / 10)
          + calcStringFormula(rtargetsalary.toString, rnowsalary.toString, entryWeight)
        })
    }

    val deliveryCountMatches = deliveryMatches.map(xs => (xs._1, 1)).reduceByKey(_+_)
    val deliverySumMatches = deliveryMatches.reduceByKey(_+_)
    val deliverySquareSumMatches = deliveryMatches
      .map(r => (r._1, r._2.toDouble * r._2.toDouble))
      .reduceByKey(_+_)
    val deliveryAveMatches = deliverySumMatches.join(deliveryCountMatches)
      .map {
        case (key, (sumByKey, cnt))
        => (key, sumByKey.toDouble / cnt)
      }
    val deliveryVarMatches = deliverySquareSumMatches.join(deliveryCountMatches)
      .map {
        case (key, (squareSum, cnt))
        => (key, (squareSum * cnt, cnt))
      }
      .join(deliverySumMatches)
      .map {
        case (key, ((nSquareSum, cnt), sum))
        => (key, (nSquareSum - sum * sum) / cnt / cnt)
      }
    val deliveryMaxMatch = deliveryMatches.reduceByKey((x,y) => if(x>y) x else y)
    val deliveryMatchesFull = deliveryMaxMatch
      .fullOuterJoin(deliveryVarMatches)
      .map {
        case (key, (max, variance)) => (key, (
          if (max.isDefined) max.get.toString else null,
          if (variance.isDefined) variance.get.toString else null
        ))
      }
      .fullOuterJoin(deliveryAveMatches)
      .map {
        case (key, (exist, ave)) => Row(
          key._1,
          key._2,
          if (exist.isDefined) exist.get._1 else null,
          if (exist.isDefined) exist.get._2 else null,
          if (ave.isDefined) ave.get.toString else null
        )
      }

    val schemaString = "resumeid deliverydate maxmatch varmatch avematch"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val namedDF = sqlContext.createDataFrame(
      deliveryMatchesFull,
      dataStructure
    )

    import sqlContext.implicits._

    namedDF.repartition($"deliverydate").write.mode("overwrite")
      .save("hdfs:///user/shuyangshi/58feature_deliverymatches")

  }

}
