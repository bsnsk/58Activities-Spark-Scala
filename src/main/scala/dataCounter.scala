import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 08/04/2017.
  * Only for some data counting job (for the thesis)
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

    val validResumes = resumeWithDates.
      filter(xs => xs._1 != null && xs._2 != null
        && xs._2.toString.toInt >= 20160910 && xs._2.toString.toInt <= 20161010)
    val numResumesPerDay = validResumes.
      map(xs => (xs._2.toString, 1)).
      reduceByKey(_+_).collect()

    printf("# BSNSK - Resumes Per Day: " + numResumesPerDay.toSeq.toString + "\n")
    printf("# BSNSK - Resumes total: " + validResumes.map(_._1).distinct.count.toString + "\n")

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

    val validPositions = positionWithDates.
      filter(xs => xs._1 != null && xs._2 != null
        && xs._2.toString.toInt >= 20160910 && xs._2.toString.toInt <= 20161010)
    val numPositionPerDay = validPositions.
      map(xs => (xs._2.toString, 1)).
      reduceByKey(_+_).collect()

    printf("# BSNSK - Positions Per Day: " + numPositionPerDay.toSeq.toString + "\n")
    printf("# BSNSK - Position total: " + validPositions.map(_._1).distinct.count.toString + "\n")

  }

  def createTablePosition(sc: SparkContext,
                          sqlContext: org.apache.spark.sql.SQLContext
                         ):Unit = {

    val textFiles = sc.textFile("hdfs:///zp/58Data/position/position_*")

    val schemaString = "infoid adddate cate1 cate2 cate3 " +
      "title postdate"
    val dataStructure = new StructType(
      schemaString.split(" ").map(fieldName =>
        StructField(fieldName, StringType, nullable = false)
      )
    )

    val rowRDD = textFiles
      .map(_.split("\001"))
      .filter(xs => xs.length >= 24)
      .map(xs => Row(xs(0), xs(1), xs(2), xs(3), xs(4),
        xs(6), xs(8)))

    val namedDF = sqlContext.createDataFrame(
      rowRDD,
      dataStructure
    )

    namedDF.registerTempTable("58data_positions")
  }

  def positionRawCount(
                      sc: SparkContext,
                      sqlContext: SQLContext
                      ): Unit = {

    createTablePosition(sc, sqlContext)

    val positionWithDates = sqlContext.sql(
      """
        SELECT DISTINCT
          r.infoid,
          FROM_UNIXTIME(SUBSTR(r.adddate, 1, 10),'YYYYMMdd')
            AS adddate,
          FROM_UNIXTIME(SUBSTR(r.postdate, 1, 10),'YYYYMMdd')
            AS postdate
        FROM 58data_positions r
        WHERE
          r.infoid <> '-'
          AND r.adddate <> '-'
          AND r.postdate <> '-'
      """.stripMargin).
      rdd.
      filter(xs => xs.length >= 3).
      map(xs => (xs(0), xs(1), xs(2)))

    val validPositions = positionWithDates.
      filter(xs => xs._1 != null && xs._2 != null
        && xs._2.toString.toInt >= 20160910 && xs._2.toString.toInt <= 20161010)
      .filter(xs => xs._2 == xs._3)
    val numPositionPerDay = validPositions.
      map(xs => (xs._2.toString, 1)).
      reduceByKey(_+_).collect()

    printf("# BSNSK - Post=Add Positions Per Day: " + numPositionPerDay.toSeq.toString + "\n")
    printf("# BSNSK - Post=Add Position total: " + validPositions.map(_._1).distinct.count.toString + "\n")

  }

  def userActivityCount(
                     sc: SparkContext,
                     sqlContext: SQLContext
                   ): Unit = {

    val textFile = sc.textFile("hdfs:///user/shuyangshi/58feature_history")
    val data = textFile.
      map(row => {
        val resumeId = row.split(",")(0).substring(1)
        val historyClicks = row.split("WrappedArray")(1).
          substring(1).split(')')(0).split(',').map(_.toDouble)
        val historyDeliveries = row.split("WrappedArray")(2).
          substring(1).split(')')(0).split(',').map(_.toDouble)
        (resumeId, (historyClicks, historyDeliveries))
      })

    val clickOrDelivery = data.map(xs => {
      val click = xs._2._1
      val delivery = xs._2._2
      val cod = (click, delivery).zipped.map((x, y) => if (x > 0 || y > 0) 1 else 0)
      (xs._1, cod)
    })

    val activityDayCount = clickOrDelivery
      .map(xs => (xs._2.sum, 1))
      .reduceByKey(_+_)
      .sortByKey()

    val continuousActivityDayCount = clickOrDelivery.flatMap(xs => {
      val act = xs._2
      var res: List[(Int, Int)] = List()
      var currentLength = 0
      for (i <- act.indices) {
        if (act(i) == 1) {
          currentLength += 1
        }
        else {
          if (currentLength > 0 && i+1 < act.length && act(i+1) == 1) {
            currentLength += 1
          }
          else {
            if (currentLength > 0) {
              res = res.:+((currentLength, 1))
            }
            currentLength = 0
          }
        }
      }
      res
    })
      .reduceByKey(_+_)
      .sortByKey()

    val longestActivityInterval = clickOrDelivery.flatMap(xs => {
      val act = xs._2
      var i = 0
      var j = act.length - 1
      while (i < act.length && act(i) == 0) {
        i += 1
      }
      while (j >= 0 && act(j) == 0) {
        j -= 1
      }
      if (i <= j) {
        List((j-i, 1))
      }
      else {
        List()
      }

    })
      .reduceByKey(_+_)
      .sortByKey()

    val longestActivityIntervalSum = longestActivityInterval.map(_._2).sum
    val longestActivityIntervalRatio = longestActivityInterval.map(xs =>
      (xs._1, xs._2 * 1.0 / longestActivityIntervalSum)
    )

    val validateDeltaAs3 = clickOrDelivery.map(xs => {
      val act = xs._2
      var res = (0, 0)
      for (i <- act.indices) {
        if (act(i) == 0) {
          val idx = if(i-5>=0) i-5 else 0
          val pastActivity = act.slice(idx, i)
          val pastActive = if (pastActivity.nonEmpty && pastActivity.max > 0) true else false
          val deltaActivity = act.slice(i, i+3)
          val deltaActive = if (deltaActivity.max > 0) true else false
          val futureActivity = act.slice(if (i+3 < act.length) i+3 else act.length, act.length)
          val futureActive = if (futureActivity.nonEmpty && futureActivity.max > 0) true else false
          if (pastActive && !deltaActive) {
            if (futureActive) {
              res = (res._1 + 1, res._2)
            }
            else {
              res = (res._1, res._2 + 1)
            }
          }
        }
      }
      (res._2, res._1 + res._2)
    })
      .reduce((x, y) => (x._1+y._1, x._2+y._2))

    val powerActivityInterval = clickOrDelivery.flatMap(xs => {
      val act = xs._2
      var res: List[(Int, Int)] = List()
      var currentLength = 0
      var currentPower = 0
      var lastActivity = 0
      var firstActivity = 0
      for (i <- act.indices) {
        if (act(i) == 1) {
          currentPower += 1
          currentLength += 1
          lastActivity = i
          if (currentLength == 1) {
            firstActivity = i
          }
        }
        else {
          if (currentPower > 0) {
            currentPower -= 1
          }
          else if (currentLength > 0) {
            res = res.:+((lastActivity - firstActivity + 1, 1))
            currentLength = 0
          }
        }
      }
      if (currentLength > 0){
        res = res.:+((lastActivity - firstActivity + 1, 1))
      }
      res
    })
      .reduceByKey(_+_)
      .sortByKey()

    printf("# BSNSK activityDayCount: " + activityDayCount.collect().toSeq.toString + "\n")
    printf("# BSNSK continuousActivityDayCount: " + continuousActivityDayCount.collect().toSeq.toString + "\n")
    printf("# BSNSK longestActivityInterval: " + longestActivityInterval.collect().toSeq.toString + "\n")
    printf("# BSNSK longestActivityIntervalRatio: " + longestActivityIntervalRatio.collect().toSeq.toString + "\n")
    printf("# BSNSK powerActivityInterval: " + powerActivityInterval.collect().toSeq.toString + "\n")
    printf("# BSNSK validDeltaAs3: " + validateDeltaAs3.toString + "\n")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    positionRawCount(sc, sqlContext)

//    resumeCount(sc, sqlContext)
//    positionCount(sc, sqlContext)
//    userActivityCount(sc, sqlContext)
  }
}
