import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Predictor: Basic LR Using org.apache.spark.ml.classification.LogisticRegression
  * Created by Ivan on 2017/1/7.
  */
object PredictorLRML extends PredictionTest {

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

    val trainingDataWithWeight = trainingData
      .map(xs => (xs._1, xs._2, if (xs._1 == 1) 1.0 else 1.0 / rate))
      .toDF("label", "feature", "weight")

    val lrModel = new LogisticRegression()
      .setWeightCol("weight")
      .setFeaturesCol("feature")
      .setLabelCol("label")
      .setMaxIter(10)
      .fit(trainingDataWithWeight)

    lrModel.transform(testData.map(xs => (xs._1, xs._2._1, xs._2._2)).toDF("date", "label", "feature"))
      .select($"date", $"label", $"prediction")
      .rdd
      .map(xs => (xs(0).toString.toDouble.toInt, (xs(1).toString.toDouble.toInt, xs(2).toString.toDouble.toInt)))
  }

}
