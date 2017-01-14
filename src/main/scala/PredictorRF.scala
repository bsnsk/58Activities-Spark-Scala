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

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(Double, Vector)],
                                       testData: RDD[(Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(
      trainingData.map(xs => LabeledPoint(xs._1, xs._2)),
      numClasses, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

    testData
      .map(data => {
        val prediction = model.predict(data._2._2)
        (data._1, (data._2._1.toInt, prediction.toInt))
      })
  }
}
