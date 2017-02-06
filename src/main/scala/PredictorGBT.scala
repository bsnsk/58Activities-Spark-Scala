import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 2017/1/7.
  */
object PredictorGBT extends PredictionTest {

  override var identifier: String = "GBT"
  override var addTimeFeature: Boolean = false

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(Double, Vector)],
                                       testData: RDD[(Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {
    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.

    var boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 5 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 6

    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(
      trainingData.map(xs => LabeledPoint(xs._1, xs._2)),
      boostingStrategy
    )

    testData
      .map(data => {
        val prediction = model.predict(data._2._2)
        (data._1, (data._2._1.toInt, prediction.toInt))
      })
  }
}
