import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 08/04/2017.
  */
object dataCounter {

  def resumeCount(
                 sc: SparkContext,
                 sqlContext: SQLContext
                 ): Unit = {
    FeatureExtractorUserAction.createTableUserResume(sc, sqlContext)
    val resumeWithDates = sqlContext.sql(
      """
        SELECT DISTINCT
          r.resumeid,
          FROM_UNIXTIME(SUBSTR(r.adddate, 1, 10),'YYYYMMdd')
            AS adddate
        FROM 58data_user_resume r
        WHERE
          r.resumeid <> '-'
      """.stripMargin).
      rdd.
      filter(xs => xs.length >= 2).
      map(xs => (xs(0), xs(1)))

    val numResumesPerDay = resumeWithDates.
      filter(xs => xs._1 != null && xs._2 != null
        && xs._2.toString.toInt >= 20160910 && xs._2.toString.toInt <= 20161010).
      map(xs => (xs._2.toString, 1)).
      reduceByKey(_+_).
      collect()

    printf("# BSNSK - Resumes Per Day: " + numResumesPerDay.toSeq.toString)

  }

  def positionCount(
                   sc: SparkContext,
                   sqlContext: SQLContext
                 ): Unit = {

    FeatureExtractorPositionDetail.createTablePosition(sc, sqlContext)
    val positionWithDates = sqlContext.sql(
      """
        SELECT DISTINCT
          r.infoid,
          FROM_UNIXTIME(SUBSTR(r.adddate, 1, 10),'YYYYMMdd')
            AS adddate
        FROM 58data_positions r
        WHERE
          r.infoid <> '-'
          AND r.adddate <> '-'
      """.stripMargin).
      rdd.
      filter(xs => xs.length >= 2).
      map(xs => (xs(0), xs(1)))

    val numPositionPerDay = positionWithDates.
      filter(xs => xs._1 != null && xs._2 != null
        && xs._2.toString.toInt >= 20160910 && xs._2.toString.toInt <= 20161010).
      map(xs => (xs._2.toString, 1)).
      reduceByKey(_+_).
      collect()

    printf("# BSNSK - Positions Per Day: " + numPositionPerDay.toSeq.toString)

  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    resumeCount(sc, sqlContext)
    positionCount(sc, sqlContext)
  }
}
