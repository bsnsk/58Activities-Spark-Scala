import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 2017/1/7.
  */
object PredictorRF extends PredictionTest {

  override var identifier: String = "RF"
  override var addTimeFeature: Boolean = false

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 50 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 64

    val model = RandomForest.trainClassifier(
      trainingData.map(xs => LabeledPoint(xs._3._1, xs._3._2)),
      numClasses, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

    testData
      .map(data => {
        val prediction = model.predict(data._3._2)
        (data._2, (data._3._1.toInt, prediction.toInt))
      })
  }
}
