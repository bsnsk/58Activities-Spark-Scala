import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.optimization.{Gradient, GradientDescent, SquaredL2Updater}
import org.apache.spark.mllib.util.MLUtils

import scala.math

/**
  * Created by Ivan on 2017/1/16.
  */
object PredictorLRSGD extends PredictionTest {

  override var identifier: String = "LRSGD"
  override var addTimeFeature: Boolean = false

  class myGradient extends Gradient {
    override def compute(data: Vector, label: Double, weights: Vector, cumGradient: Vector): Double = {

      val margin = -1.0 * (data.toArray, weights.toArray).zipped.map(_*_).sum
      val multiplier = (1.0 / (1.0 + math.exp(margin))) - label
      //y += a * x，即cumGradient += multiplier * data
      var accumulativeGradient = cumGradient.toDense.values
      data.foreachActive((i, feature) => {
        accumulativeGradient(i) += multiplier * feature
      })
      if (label > 0.5) {
        math.log(1 + math.exp(margin))
      } else {
        math.log(1 + math.exp(margin)) - margin
      }

    }
  }

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {
    val convergenceTol = 1e-4
    val numIterations = 10
    val regParam = 0.01
    val initialWeights = Vectors.dense(Array.fill(numberOfFeatures)(0.0))
    val gradient = new myGradient()

    val cntPositiveSamples = trainingData.filter(r => r._3._1 == 1).count()
    val cntNegativeSamples = trainingData.filter(r => r._3._1 == 0).count()
    val rate = (cntNegativeSamples.toDouble / cntPositiveSamples.toDouble).toInt

    val weights = GradientDescent
      .runMiniBatchSGD(
        trainingData.flatMap(xs =>
          if (xs._3._1.toInt == 1) List.fill(rate)(xs._3)
          else List(xs._3)
        ),
        gradient,
        new SquaredL2Updater(),
        stepSize = 0.1,
        numIterations,
        regParam,
        miniBatchFraction = 1.0,
        initialWeights,
        convergenceTol
      )._1

    val calcScore: Vector => Double = features => 1.0 / (1.0 + scala.math.exp(
      - (weights.toArray, features.toArray).zipped.map(_*_).sum
    ))

    testData
      .map(data => {
        val prediction = if (calcScore(data._3._2) > 0.5) 1 else 0
        (data._2, (data._3._1.toInt, prediction.toInt))
      })

  }
}
