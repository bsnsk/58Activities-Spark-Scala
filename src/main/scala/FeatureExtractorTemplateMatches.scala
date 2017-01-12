import breeze.numerics.abs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Template Class for Action Matches (between positions and resumes)
  * Providing
  *   - matching feature list pattern string,
  *   - JOINs to acquire the feature list, and
  *   - functions to calculate feature Dataframes.
  *
  * Created by Ivan on 2017/1/5.
  */
class FeatureExtractorTemplateMatches extends java.io.Serializable {

  val featureString: String =
    "max_match_rp_salary max_match_rp_education max_match_r_complete max_match_rr_salary max_match_rp_gender " +
    "var_match_rp_salary var_match_rp_education var_match_r_complete var_match_rr_salary var_match_rp_gender " +
    "ave_match_rp_salary ave_match_rp_education ave_match_r_complete ave_match_rr_salary ave_match_rp_gender"

  val entryWeight: ((Int, Int) => Double) = (x: Int, y: Int) => abs(x-y).toDouble

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

  def calcGenderMatchWeight(genderResStr: String, genderPos: Int): Double = {
    if (genderResStr == "-")
      if (genderPos == -1) 0
      else genderPos * 0.2 - 0.1
    else if (genderPos == -1) genderPos - 0.5
      else if (genderPos == genderResStr.toInt) 1
        else -1
  }

  def calcMatchFeatureListsWithPositionId(
                             actionByResume: RDD[(Any, (Any, Any))],
                             sqlContext: org.apache.spark.sql.SQLContext,
                             debug: Boolean = false
                           ): RDD[((Any, Any), List[Double])] = {
    val tableResumeDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumes/*.parquet")
    val tablePositionDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_positions/*.parquet")

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
          xs(9),
          xs(10)
        )
      ))

    if (debug)
      println("[DEBUG] # of Actions: " + actionByResume.count().toString)

    val matchesHalf = actionByResume.join(dataResumeDetail)
      .map {
        case (resumeid, (
            (positionid, date),
            (nowposition, targetcategory, targetposition,
              targetsalary, education, gender,
              jobstate, areaid, complete, nowsalary
            )
          )) =>
          (positionid, (
            resumeid,
            date,
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

    if (debug)
      println("[DEBUG] # of ActionJoinedByResumes: " + matchesHalf.count().toString)

    val matches = matchesHalf
      .join(dataPositionDetail)
      .map {
        case (positionid, (
        ( resumeid,
          rdate,
          rnowposition,
          rtargetcategory,
          rtargetposition,
          rtargetsalary,
          reducation,
          rgender,
          rjobstate,
          rareaid,
          rcomplete,
          rnowsalary
        ),
        ( padddate,
          pcategory1,
          pcategory2,
          pcategory3,
          ptitle,
          psalary,
          peducation,
          pexperience,
          ptrade,
          entid
        )
        )) =>
          ( (resumeid, rdate),
            List(calcStringFormula(rtargetsalary.toString, psalary.toString, entryWeight)
              , calcStringFormula(reducation.toString, peducation.toString, entryWeight)
              , calcStringFormula(rcomplete.toString, "", (x:Int, _:Int) => x.toDouble / 10)
              , calcStringFormula(rtargetsalary.toString, rnowsalary.toString, entryWeight)
              , calcGenderMatchWeight(
                  rgender.toString,
                  if (ptitle.toString.indexOf("男") != -1) 0
                  else if (ptitle.toString.indexOf("女") != -1) 1
                  else -1
              )
            )
          )
      }
    if (debug)
      println("[DEBUG] # of ActionJoinedByResumesAndPositions: " + matches.count().toString)

    matches
  }

  def calcMatchFeatureListsWithEntUserId(
                                           actionByResume: RDD[(Any, (Any, Any))],
                                           sqlContext: org.apache.spark.sql.SQLContext,
                                           debug: Boolean = false
                                         ): RDD[((Any, Any), List[Double])] = {
    val tableResumeDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_resumes/*.parquet")
    val tablePositionDetail = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_positions/*.parquet")

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
        xs(10),
        (
          xs(0),
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

    if (debug)
      println("[DEBUG] # of Actions: " + actionByResume.count().toString)

    val matchesHalf = actionByResume.join(dataResumeDetail)
      .map {
        case (resumeid, (
          (entuserid, date),
          (nowposition, targetcategory, targetposition,
          targetsalary, education, gender,
          jobstate, areaid, complete, nowsalary
            )
          )) =>
          (entuserid, (
            resumeid,
            date,
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

    if (debug)
      println("[DEBUG] # of ActionJoinedByResumes: " + matchesHalf.count().toString)

    val matches = matchesHalf
      .join(dataPositionDetail)
      .map {
        case (entuserid, (
          ( resumeid,
          rdate,
          rnowposition,
          rtargetcategory,
          rtargetposition,
          rtargetsalary,
          reducation,
          rgender,
          rjobstate,
          rareaid,
          rcomplete,
          rnowsalary
            ),
          ( positionid,
          padddate,
          pcategory1,
          pcategory2,
          pcategory3,
          ptitle,
          psalary,
          peducation,
          pexperience,
          ptrade
            )
          )) =>
          ( (resumeid, rdate),
            List(calcStringFormula(rtargetsalary.toString, psalary.toString, entryWeight)
              , calcStringFormula(reducation.toString, peducation.toString, entryWeight)
              , calcStringFormula(rcomplete.toString, "", (x:Int, _:Int) => x.toDouble / 10)
              , calcStringFormula(rtargetsalary.toString, rnowsalary.toString, entryWeight)
              , calcGenderMatchWeight(
                rgender.toString,
                if (ptitle.toString.indexOf("男") != -1) 0
                else if (ptitle.toString.indexOf("女") != -1) 1
                else -1
              )
            )
          )
      }
    if (debug)
      println("[DEBUG] # of ActionJoinedByResumesAndPositions: " + matches.count().toString)

    matches
  }
  def calcMatchStatisticsFeatures(matches: RDD[((Any, Any), List[Double])]): RDD[Row] = {
    val countMatches = matches.map(xs => (xs._1, 1.toDouble)).reduceByKey(_+_)
    val sumMatches = matches.reduceByKey((xs, ys) => (xs, ys).zipped.map(_+_))
    val squareSumMatches = matches
      .map(r => (
        r._1,
        r._2.map(x => x * x)
      ))
      .reduceByKey((xs, ys) => (xs, ys).zipped.map(_+_))
    val aveMatches = sumMatches.join(countMatches)
      .map {
        case (key, (sumByKey, cnt))
        => (key, sumByKey.map(_ / cnt))
      }
    val varMatches = squareSumMatches.join(countMatches)
      .map {
        case (key, (squareSum, cnt))
        => (key, (squareSum.map(_ * cnt), cnt))
      }
      .join(sumMatches)
      .map {
        case (key, ((nSquareSum, cnt), sum))
        => (key, (nSquareSum, sum).zipped.map((x, y) => (x - y * y) / cnt / cnt))
      }
    val maxMatch = matches.reduceByKey(
      (xs, ys) => (xs, ys).zipped.map((x, y) => if(x>y) x else y)
    )
    val matchesFull = maxMatch
      .join(varMatches)
      .map(xs => (xs._1, xs._2._1 ++ xs._2._2))
      .join(aveMatches)
      .map(xs => Row.fromSeq(
        xs._1._1 :: xs._1._2 :: (xs._2._1 ++ xs._2._2).map(_.toString)
      ))
    matchesFull
  }
}
