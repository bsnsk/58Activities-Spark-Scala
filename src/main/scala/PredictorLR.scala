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

  def predictionResultLabelsAndScores(
                       trainingData: RDD[(Double, Vector)],
                       testData: RDD[(Int, (Double, Vector))],
                       sqlContext: org.apache.spark.sql.SQLContext
                     ): RDD[(Int, (Int, Int))] = {

    val numIterations = 10
    val lrModel = LogisticRegressionWithSGD.train(
      trainingData.map(xs => LabeledPoint(xs._1, xs._2)),
      numIterations
    )
    testData
      .map(data => {
        val prediction = lrModel.predict(data._2._2)
        (data._1, (data._2._1.toInt, prediction.toInt))
      })
  }

}
