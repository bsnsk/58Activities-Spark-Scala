import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Predictor: Basic LR Using org.apache.spark.mllib.classification.LogisticRegression
  * Created by Ivan on 2016/12/12.
  */
object PredictorLR extends PredictionTest {

  override var identifier: String = "LR"
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

    val numIterations = 10
    val lrModel = LogisticRegressionWithSGD.train(
      trainingDataFeed,
      numIterations
    )
    testData
      .map(data => {
        val prediction = lrModel.predict(data._3._2)
        (data._2, (data._3._1.toInt, prediction.toInt))
      })
  }

}
