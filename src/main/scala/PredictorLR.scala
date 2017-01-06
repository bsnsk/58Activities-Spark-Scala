import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Predictor: Basic LR
 * Created by Ivan on 2016/12/12.
 */
object PredictorLR extends PredictionTest {

  def predictionResultLabelsAndScores(
                       trainingData: RDD[LabeledPoint],
                       testData: RDD[(Int, LabeledPoint)]
                     ): RDD[(Int, (Int, Int))] = {

    val numIterations = 10
    val lrModel = LogisticRegressionWithSGD.train(
      trainingData,
      numIterations
    )
    testData
      .map(data => {
        val prediction = lrModel.predict(data._2.features)
        (data._1, (data._2.label.toInt, prediction.toInt))
      })
  }

}
