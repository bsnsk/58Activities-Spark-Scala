import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.optimization.{Gradient, GradientDescent, SquaredL2Updater}
import org.apache.spark.mllib.util.MLUtils

import scala.math

/**
  * Created by Ivan on 2017/2/6.
  */
object PredictorTiLR extends PredictionTest {

  override var identifier: String = "TiLR"
  override var addTimeFeature: Boolean = true

  class myGradient extends Gradient {
    override def compute(data: Vector, label: Double, weights: Vector, cumGradient: Vector): Double = {

      val margin = -1.0 * (data.toArray, weights.toArray).zipped.map(_*_).sum
      val multiplier = (1.0 / (1.0 + math.exp(margin))) - label

      val numFeatures = data.size
      val dateParameter = 0.1
      val dateMultiplier = math.exp(- dateParameter * (maxDayOfYear - data(numFeatures - 1)))

      //y += a * x，即cumGradient += multiplier * data
      var accumulativeGradient = cumGradient.toDense.values
      data.foreachActive((i, feature) => {
        if (i < numFeatures - 1) {
          accumulativeGradient(i) += multiplier * feature * dateMultiplier
        }
      })
      if (label > 0.5) {
        dateMultiplier * math.log(1 + math.exp(margin))
      } else {
        dateMultiplier * (math.log(1 + math.exp(margin)) - margin)
      }

    }
  }

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(Double, Vector)],
                                       testData: RDD[(Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {
    val convergenceTol = 1e-4
    val numIterations = 10
    val regParam = 0.01
    val initialWeights = Vectors.dense(Array.fill(trainingData.first()._2.size - 1)(0.0))
    val gradient = new myGradient()

    val cntPositiveSamples = trainingData.filter(r => r._1 == 1).count()
    val cntNegativeSamples = trainingData.filter(r => r._1 == 0).count()
    val rate = (cntNegativeSamples.toDouble / cntPositiveSamples.toDouble).toInt

    val weights = GradientDescent
      .runMiniBatchSGD(
        trainingData.flatMap(xs =>
          if (xs._1.toInt == 1) List.fill(rate)(xs)
          else List(xs)
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
        val prediction = if (calcScore(data._2._2) > 0.5) 1 else 0
        (data._1, (data._2._1.toInt, prediction.toInt))
      })

  }
}
