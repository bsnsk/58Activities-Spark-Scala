import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 2017/1/7.
  */
object PredictorGBTML extends PredictionTest {

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(Double, Vector)],
                                       testData: RDD[(Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {
    import sqlContext.implicits._
    val cntPositiveSamples = trainingData.filter(r => r._1 == 1).count()
    val cntNegativeSamples = trainingData.filter(r => r._1 == 0).count()
    val rate = cntNegativeSamples.toDouble / cntPositiveSamples.toDouble

    println("BSNSK #rate = " + rate.toString + "#")

    val trainingDataDF = trainingData.toDF("label", "feature")

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

    val gbt = new GBTClassifier()
      .setFeaturesCol("indexedFeature")
      .setLabelCol("indexedLabel")
      .setMaxIter(10) // TODO: Parameter

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    val model = pipeline.fit(trainingDataDF)

    model.transform(testData.map(xs => (xs._1, xs._2._1, xs._2._2)).toDF("date", "label", "feature"))
      .select($"date", $"label", $"predictedLabel")
      .rdd
      .map(xs => (xs(0).toString.toDouble.toInt, (xs(1).toString.toDouble.toInt, xs(2).toString.toDouble.toInt)))
  }
}
