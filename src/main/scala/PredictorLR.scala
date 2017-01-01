import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

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

    val labeledData = sc.textFile("hdfs:///user/shuyangshi/58data_labeledNoSQL/part-*")

    val data = labeledData
      .map(r => {
        val date = r.split(',')(0).split('[')(1)
        val array = r.split('(')(1).split(')')(0).split(", ")
        val activeness = array(0)
        val features = array.slice(1, array.size).map(_.toDouble)
        (date.toInt, LabeledPoint(activeness.toInt, Vectors.dense(features)))
      })

    val dividerDate = 20161006
    val trainingData = data.filter(pair => pair._1 <= dividerDate)
      .map(r => r._2)
    val testData = data.filter(pair => pair._1 >= dividerDate)

    val numIterations = 10
    val lrModel = LogisticRegressionWithSGD.train(trainingData, numIterations)
    val closedTest = testData
      .map(data => {
        val prediction = lrModel.predict(data._2.features)
        (data._1, (data._2.label.toInt, prediction.toInt))
      })
      .map {
        case (date, (1, 1)) => (date, (1, 0, 0, 0)) // TP
        case (date, (0, 1)) => (date, (0, 1, 0, 0)) // FP
        case (date, (0, 0)) => (date, (0, 0, 1, 0)) // TN
        case (date, (1, 0)) => (date, (0, 0, 0, 1)) // FN
      }
      .reduceByKey((xs, ys) => (
        xs._1 + ys._1,
        xs._2 + ys._2,
        xs._3 + ys._3,
        xs._4 + ys._4
      ))
      .sortByKey(ascending = true, numPartitions = 1)
      .map(xs => {
        val date = xs._1
        val TP = xs._2._1.toDouble
        val FP = xs._2._2.toDouble
        val TN = xs._2._3.toDouble
        val FN = xs._2._4.toDouble
        val P = TP / (TP + FP)
        val R = TP / (TP + FN)
        val F1 = 2 * P * R / (P + R)
        (date, P, R, F1, xs._2)
      })

    val outputPath = "hdfs:///user/shuyangshi/58prediction_test"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)

    closedTest.saveAsTextFile(outputPath)
  }
}
