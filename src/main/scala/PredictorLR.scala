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

    val cntPositiveSamples = trainingData.filter(r => r._3._1 == 1).count()
    val cntNegativeSamples = trainingData.filter(r => r._3._1 == 0).count()
    val rate = (cntNegativeSamples.toDouble / cntPositiveSamples.toDouble).toInt

    val numIterations = 10
    val lrModel = LogisticRegressionWithSGD.train(
      trainingData.flatMap(xs =>
        if (xs._3._1.toInt == 1) List.fill(rate)(LabeledPoint(xs._3._1, xs._3._2))
        else List(LabeledPoint(xs._3._1, xs._3._2)
      )),
      numIterations
    )
    testData
      .map(data => {
        val prediction = lrModel.predict(data._3._2)
        (data._2, (data._3._1.toInt, prediction.toInt))
      })
  }

}
