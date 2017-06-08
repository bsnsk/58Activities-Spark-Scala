import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 17/05/2017.
  */
object PredictorWithSpecifiedDataSVM extends PredictorWithSpecifiedData {

  override var featureTag: String = "B"
  override var identifier: String = "WithSpecifiedDataSVM"

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

    val numIterations = 20
    val model = SVMWithSGD.train(
      trainingData.map(xs => LabeledPoint(xs._3._1, xs._3._2)),
      numIterations
    )

    val baseDate = dataDivideDate
    testData.map(data => {
      val prediction = model.predict(data._3._2)
      (baseDate + data._2, (data._3._1.toInt, prediction.toInt))
    })
  }
}
