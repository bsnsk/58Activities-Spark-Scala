import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 17/05/2017.
  */
abstract class PredictorWithSpecifiedData {

  var identifier: String
  var featureTag: String
  var maxDayOfYear: Double = -1

  var numberOfFeatures: Int = -1
  var dataDivideDate = 20161005

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))]

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val data = DataAssistant.prepareData(featureTag, sc)
    val dividerDate = dataDivideDate
    val training = data.filter(pair => pair._2 <= dividerDate && pair._2 > 0)
      .map(r => (r._1, r._2.toInt, r._3))
    val test = data.filter(pair => pair._2 >= dividerDate && pair._2 > 0 && pair._2 <= 20181010)
      .map(r => (r._1, r._2.toInt, r._3))

    val testLabelsAndScores = predictionResultLabelsAndScores(training, test, sqlContext)

    evalPredictionResult(sc, testLabelsAndScores)
  }

  def evalPredictionResult(sc: SparkContext, testLabelsAndScores: RDD[(Int, (Int, Int))]): Unit = {

    val dates = testLabelsAndScores.map(xs => xs._1).distinct().collect()
    var auROCList: List[(Int, Double)] = List()
    for (date <- dates) {
      val auROC = new BinaryClassificationMetrics(
        testLabelsAndScores.filter(_._1 == date)
          .map(xs => (xs._2._2, xs._2._1))
      ).areaUnderROC()
      auROCList = (date, auROC) :: auROCList
    }
    val auROCs = sc.makeRDD(auROCList)

    val f1MeasureScores = testLabelsAndScores
      .map {
        case (date, (1, 1)) => (date, (1, 0, 0, 0)) // TP
        case (date, (0, 1)) => (date, (0, 1, 0, 0)) // FP
        case (date, (0, 0)) => (date, (0, 0, 1, 0)) // TN
        case (date, (1, 0)) => (date, (0, 0, 0, 1)) // FN
      }
      .reduceByKey((xs, ys) => (
        xs._1 + ys._1,
        xs._2 + ys._2,
        xs._3 + ys._3,
        xs._4 + ys._4
      ))
      .map(xs => {
        val date = xs._1
        val TP = xs._2._1.toDouble
        val FP = xs._2._2.toDouble
        val TN = xs._2._3.toDouble
        val FN = xs._2._4.toDouble
        val P = TP / (TP + FP)
        val R = TP / (TP + FN)
        val F1 = 2 * P * R / (P + R)
        (date, (P, R, F1, xs._2))
      })

    val testMeasures = auROCs.join(f1MeasureScores)
      .sortByKey(ascending = true, numPartitions = 1)

    val outputPath = "hdfs:///user/shuyangshi/58PredictionTest" + identifier
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)

    testMeasures.saveAsTextFile(outputPath)

  }

}
