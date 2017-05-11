import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by Ivan on 14/04/2017.
  */
object PredictorHistorySVM extends PredictorWithHis {

  override var identifier: String = "HistorySVM"
  override var addTimeFeature: Boolean = false

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

    val positiveSamples = trainingData.filter(r => r._3._1 == 1)
    val negativeSamples = trainingData.filter(r => r._3._1 == 0)
    val rate = positiveSamples.count().toDouble / negativeSamples.count().toDouble
    val trainingDataFeed = negativeSamples.sample(false, rate)
      .union(positiveSamples).map(xs => LabeledPoint(xs._3._1, xs._3._2))

    val numIterations = 100
    val model = SVMWithSGD.train(trainingDataFeed, numIterations)

    val baseDate = dataDivideDate
    testData.map(data => {
      val prediction = model.predict(data._3._2)
      (baseDate + data._2, (data._3._1.toInt, prediction.toInt))
    })
  }
  override
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val (training, test) = PredictorHistory.acquireDividedHistoryData(sc)

    val testLabelsAndScores = predictionResultLabelsAndScores(training, test, sqlContext)

    evalPredictionResult(sc, testLabelsAndScores)
  }
}
