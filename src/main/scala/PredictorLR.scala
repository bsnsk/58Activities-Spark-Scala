import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Predictor: Basic LR
 * Created by Ivan on 2016/12/12.
 */
object PredictorLR {

  /* MAIN ENTRANCE */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val tableFeatures = sqlContext.read.parquet("hdfs:///user/shuyangshi/58feature_all/*.parquet")
    val tableLabels = sqlContext.read.parquet("hdfs:///user/shuyangshi/58label_activeness")
    tableFeatures.registerTempTable("58data_features")
    tableLabels.registerTempTable("58data_labels")

    val trainingData = sqlContext.sql(
      """
        SELECT
          label.activeness,
          feature.*
        FROM 58data_labels label
        LEFT OUTER JOIN 58data_features feature
        ON label.resumeid = feature.resumeid
        AND label.date = feature.date + 1
      """.stripMargin)
      .rdd.map(r=> {
        val seq = r.toSeq
        val label = seq(1).toString.toInt
        val features = seq.slice(3, seq.size).map(_.toString.toDouble).toArray
        LabeledPoint(label, Vectors.dense(features))
      })


    val numIterations = 10
    val lrModel = LogisticRegressionWithSGD.train(trainingData, numIterations)
    val prediction = lrModel.predict(trainingData.first().features)

    println(prediction.toString)
  }
}
