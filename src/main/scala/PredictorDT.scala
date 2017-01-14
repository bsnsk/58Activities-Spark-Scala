import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 2017/1/13.
  */
object PredictorDT extends PredictionTest {

  override var identifier: String = "DT"

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(Double, Vector)],
                                       testData: RDD[(Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 8
    val maxBins = 64

    val model = DecisionTree.trainClassifier(
      trainingData.map(xs => LabeledPoint(xs._1, xs._2)),
      numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins
    )

    testData
      .map(data => {
        val prediction = model.predict(data._2._2)
        (data._1, (data._2._1.toInt, prediction.toInt))
      })

  }
}
