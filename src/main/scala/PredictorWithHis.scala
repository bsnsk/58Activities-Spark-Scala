import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 31/03/2017.
  */
abstract class PredictorWithHis extends PredictionTest {

  var KHistoryLength: Int = 5 // TODO history length

  def appendHistoryDataToRDD(
                              rdd: RDD[(String, Int, (Double, Vector))],
                              historyData: RDD[(String, (Array[Double], Array[Double]))],
                              dates: Array[Int],
                              K: Int
                            ): RDD[(String, Int, (Double, Vector))] = {
    val rddDates = rdd.map(_._2).distinct.collect()
    val nF = rdd.first()._3._2.size
    println("#BSNSK nF = " + nF.toString)
    rdd.map(xs => (xs._1, (xs._2, xs._3))).rightOuterJoin(historyData).flatMap{
      case (resumeId, (None, history)) => {
        var lst = List(): List[(Boolean, String, Int, (Double, Vector))]
        for (date <- rddDates) {
          var i = 0
          while (i < dates.length && dates(i) != date) {
            i+=1
          }
          if (i >= dates.length || i >= history._1.length || i >= history._2.length) {
            lst = lst.:+((false, resumeId, -1, (1.0, Vectors.dense(Array():Array[Double]))))
          } else {
            var historyActive:Boolean = false
            var features: Array[Double] = Array.fill(if (addTimeFeature) nF-1 else nF)(0.0)
            for (j <- 0.until(K)) {
              // i - (K-j-1)
              val d = i + j - K + 1 // i - K + 1 ~ i
              if (d >= 0 && d < history._1.length) {
                features = features.:+(history._1(d))
                features = features.:+(history._2(d))
                if (history._1(d) >= 3 || history._2(d) > 0) {
                  historyActive = true
                }
              }
              else {
                features = features.:+(0.0)
                features = features.:+(0.0)
              }
            }
            var flag = 0.0
            for (j <- 0.until(K)) {
              if (i+j+1 < history._1.length && i+j+1 < history._2.length
                  && (history._1(i+j+1) >= 3 || history._2(i+j+1) > 0))
                flag = 1.0
            }
            val newFeatures = Vectors.dense(if (addTimeFeature) features.:+(i * 1.0) else features)
            lst = lst.:+((historyActive, resumeId, date, (flag, newFeatures)))
          }
        }
        lst
      }
      case (resumeId, (dataNotSure, history)) => {
        val data = dataNotSure.get
        val date = data._1
        var i = 0
        while (i < dates.length && dates(i) != date) {
          i+=1
        }
        if (i >= dates.length || i >= history._1.length) {
          List((false, resumeId, -1, (1.0, data._2._2)))
        } else {
          var features: Array[Double] = data._2._2.toArray
          val dateFeature = if (addTimeFeature) data._2._2(data._2._2.size - 1) else 0.0
          if (addTimeFeature) {
            features = features.slice(0, features.length - 1)
          }
//          features = Array(features(2), features(9), features(10), features(11)) ++ Array.fill(nF-4)(0.0)
//          features = features.slice(0, 2) ++ Array.fill(nF-2)(0.0) //Array.fill(nF)(0.0) // TODO WARNING WHATEVER
          var historyActive = false
          for (j <- 0.until(K)) {
            // i - (K-j-1)
            val d = i + j - K + 1 // i - K + 1 ~ i
            if (d >= 0) {
              features = features.:+(history._1(d))
              features = features.:+(history._2(d))
              if (history._1(d) >= 3 || history._2(d) > 0) {
                historyActive = true
              }
            }
            else {
              features = features.:+(0.0)
              features = features.:+(0.0)
            }
          }
          List((historyActive, resumeId, date, (data._2._1, Vectors.dense(
            if (addTimeFeature) features.:+(i * 1.0) else features
          ))))
        }
      }
    }
      .filter(xs => xs._1 && xs._3 != -1)
      .map(xs => (xs._2, xs._3, xs._4))
  }

  def appendHistoryData(
                         training: RDD[(String, Int, (Double, Vector))],
                         test: RDD[(String, Int, (Double, Vector))],
                         sc: SparkContext
                       ): (RDD[(String, Int, (Double, Vector))], RDD[(String, Int, (Double, Vector))]) = {

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

    val K = KHistoryLength

    val dates = training.map(_._2).union(test.map(_._2)).distinct()
      .filter(_>=20160101).filter(_<=20180101)
      .collect().sorted

    (
      appendHistoryDataToRDD(training, data, dates, K),
      appendHistoryDataToRDD(test, data, dates, K)
    )

  }

  override
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val (training, test) = acquireDividedData(sc)
    val (updatedTraining, updatedTest) = appendHistoryData(training, test, sc)

    val testLabelsAndScores = predictionResultLabelsAndScores(updatedTraining, updatedTest, sqlContext)

    evalPredictionResult(sc, testLabelsAndScores)
  }
}
