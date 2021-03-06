import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Prediction test template for different predictors
  * Predictors need only to implement function `predictionResultLabelsAndScores`
  * Created by Ivan on 2017/1/5.
  */
abstract class PredictionTest extends Serializable {

  var identifier: String
  var maxDayOfYear: Double = -1
  var addTimeFeature: Boolean

  var numberOfFeatures: Int = -1
  var dataDivideDate = 20161005

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))]

  def acquireDividedData(sc: SparkContext): (
      RDD[(String, Int, (Double, Vector))],
      RDD[(String, Int, (Double, Vector))]
    ) = {
    val labeledData = sc.textFile("hdfs:///user/shuyangshi/58data_labeledNoSQL/part-*")

    val validIds = labeledData
      .map(r => {
        val id = r.split(',')(0).split('[')(1)
        (id, 1)
      })
      .reduceByKey(_+_)
      .filter(_._2 > 3)
    val allData = labeledData
      .map(r => {
        val id = r.split(',')(0).split('[')(1)
        val date = r.split(',')(1)
        val array = r.split('(')(1).split(')')(0).split(", ")
        val activeness = array(0)

        try {
          val features = array.slice(1, array.size).map(_.toDouble)
          val sdf = new SimpleDateFormat("yyyyMMdd")
          val dateInstance = Calendar.getInstance()
          dateInstance.setTime(sdf.parse(date))
          val dayOfYear = dateInstance.get(Calendar.DAY_OF_YEAR).toDouble
          (id.toString, (date.toLong, (activeness.toDouble,
            if (addTimeFeature) Vectors.dense(features :+ dayOfYear)
            else Vectors.dense(features)
          )))
        }
        catch {
          case _: Throwable =>
            (id.toString, (-1, (0.toDouble,
              Vectors.dense(Array.fill(array.size)(0.0))
            )))
        }
      }: (String, (Long, (Double, Vector))))

    val data = allData.join(validIds).map {
      case (id, (d, _)) => (id, d._1, d._2)
    }: RDD[(String, Long, (Double, Vector))]

    println("#BSNSK calculated numOfFeatures is " + numberOfFeatures.toString)

    val dividerDate = dataDivideDate
    val trainingData = data.filter(pair => pair._2 <= dividerDate && pair._2 > 0)
      .map(r => (r._1, r._2.toInt, r._3))
    val testData = data.filter(pair => pair._2 >= dividerDate && pair._2 > 0 && pair._2 <= 20181010)
      .map(r => (r._1, r._2.toInt, r._3))

    (trainingData, testData)
  }

  def evalPredictionResult(sc: SparkContext, testLabelsAndScores: RDD[(Int, (Int, Int))]): Unit = {

    val dates = testLabelsAndScores.map(xs => xs._1).distinct().collect()
    var auROCList: List[(Int, Double)] = List()
    for (date <- dates) {
      val auROC = new BinaryClassificationMetrics(
        testLabelsAndScores.filter(_._1 == date)
          .map(xs => (xs._2._2, xs._2._1))
      ).areaUnderROC()
      auROCList = (date, auROC) :: auROCList
    }
    val auROCs = sc.makeRDD(auROCList)

    val f1MeasureScores = testLabelsAndScores
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
      .map(xs => {
        val date = xs._1
        val TP = xs._2._1.toDouble
        val FP = xs._2._2.toDouble
        val TN = xs._2._3.toDouble
        val FN = xs._2._4.toDouble
        val P = TP / (TP + FP)
        val R = TP / (TP + FN)
        val F1 = 2 * P * R / (P + R)
        (date, (P, R, F1, xs._2))
      })

    val testMeasures = auROCs.join(f1MeasureScores)
      .sortByKey(ascending = true, numPartitions = 1)

    val outputPath = "hdfs:///user/shuyangshi/58PredictionTest" + identifier
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)

    testMeasures.saveAsTextFile(outputPath)

  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val (training, test) = acquireDividedData(sc)

    val testLabelsAndScores = predictionResultLabelsAndScores(training, test, sqlContext)

    evalPredictionResult(sc, testLabelsAndScores)
  }

}
