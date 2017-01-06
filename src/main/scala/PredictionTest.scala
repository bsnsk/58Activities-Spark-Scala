import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Prediction test template for different predictors
  * Predictors need only to implement function `predictionResultLabelsAndScores`
  * Created by Ivan on 2017/1/5.
  */
abstract class PredictionTest {

  def predictionResultLabelsAndScores(
                                   trainingData: RDD[LabeledPoint],
                                   testData: RDD[(Int, LabeledPoint)]
                                 ): RDD[(Int, (Int, Int))]

  def acquireDividedData(sc: SparkContext): (RDD[LabeledPoint], RDD[(Int, LabeledPoint)]) = {
    val labeledData = sc.textFile("hdfs:///user/shuyangshi/58data_labeledNoSQL/part-*")

    val data = labeledData
      .map(r => {
        val date = r.split(',')(0).split('[')(1)
        val array = r.split('(')(1).split(')')(0).split(", ")
        val activeness = array(0)
        val features = array.slice(1, array.size).map(_.toDouble)
        (date.toInt, LabeledPoint(activeness.toDouble.toInt, Vectors.dense(features)))
      })

    val dividerDate = 20161005
    val trainingData = data.filter(pair => pair._1 <= dividerDate)
      .map(r => r._2)
    val testData = data.filter(pair => pair._1 >= dividerDate)

    //    val cntPositiveSamples = trainingData.filter(r => r.label.toInt == 1).count()
    //    val cntNegativeSamples = trainingData.filter(r => r.label.toInt == 0).count()
    //    val rate = (cntNegativeSamples.toDouble / cntPositiveSamples).toInt
    val trainingDataBalanced = trainingData // TODO
    (trainingDataBalanced, testData)
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

    val outputPath = "hdfs:///user/shuyangshi/58prediction_test"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)

    testMeasures.saveAsTextFile(outputPath)

  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)

    val dataSet = acquireDividedData(sc)

    val testLabelsAndScores = predictionResultLabelsAndScores(dataSet._1, dataSet._2)

    evalPredictionResult(sc, testLabelsAndScores)
  }
}
