import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 2017/1/7.
  */
object PredictorDTML extends PredictionTest {

  override var identifier: String = "DTML"
  override var addTimeFeature: Boolean = false

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {
    import sqlContext.implicits._
    val cntPositiveSamples = trainingData.filter(r => r._3._1 == 1).count()
    val cntNegativeSamples = trainingData.filter(r => r._3._1 == 0).count()
    val rate = cntNegativeSamples.toDouble / cntPositiveSamples.toDouble

    println("BSNSK #rate = " + rate.toString + "#")

    val trainingDataDF = trainingData.map(_._3).toDF("label", "feature")

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainingDataDF)

    val featureIndexer = new VectorIndexer()
      .setInputCol("feature")
      .setOutputCol("indexedFeature")
      .fit(trainingDataDF)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeature")
      .setMaxDepth(10) // TODO: Parameter

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val model = pipeline.fit(trainingDataDF)

    model.transform(testData.map(xs => (xs._2, xs._3._1, xs._3._2)).toDF("date", "label", "feature"))
      .select($"date", $"label", $"predictedLabel")
      .rdd
      .map(xs => (xs(0).toString.toDouble.toInt, (xs(1).toString.toDouble.toInt, xs(2).toString.toDouble.toInt)))
  }
}
