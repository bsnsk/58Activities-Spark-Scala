import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}

/**
  * Created by Ivan on 27/03/2017.
  */
object PredictorHistory extends PredictionTest {

  override var identifier: String = "History"
  override var addTimeFeature: Boolean = false

  def acquireDividedHistoryData(sc: SparkContext): (
      RDD[(String, Int, (Double, Vector))],
      RDD[(String, Int, (Double, Vector))]
    ) = {
    val featureLength = 5 // TODO
    val deliveryWeight = 5
    val K = 3
    val clickThreshold = 3

    val labeledData = sc.textFile("hdfs:///user/shuyangshi/58data_labeledNoSQL/part-*")

    val validIds = labeledData.
      map(r => {
        val id = r.split(',')(0).split('[')(1)
        (id, 1)
      }).
      reduceByKey(_+_).
      filter(_._2 > 3)

    val textFile = sc.textFile("hdfs:///user/shuyangshi/58feature_history")
    val data = textFile.
      map(row => {
        val resumeId = row.split(",")(0).substring(1)
        val historyClicks = row.split("WrappedArray")(1).
          substring(1).split(')')(0).split(',').map(_.toDouble)
        val historyDeliveries = row.split("WrappedArray")(2).
          substring(1).split(')')(0).split(',').map(_.toDouble)
        (resumeId, (historyClicks, historyDeliveries))
      }).
      join(validIds).map {
      case (resumeId, (history, _)) => (resumeId, history._1, history._2)
      }

    val training = data
      .flatMap(row => {
        val hClicks = row._2
        val hDeliveries = row._3
        val L = hClicks.length
        var dataRows: List[(String, Int, (Double, Vector))] = List()
        for (i <- 0.until(L-featureLength-7)) { // i + fL - 1 < L - 8
          val feature: Array[Double] = Array.fill(featureLength * 2)(0.0)
          var historyActive: Boolean = false
          for (j <- 0.until(featureLength)) {
//            feature(j) = hClicks(i+j) + (if (hDeliveries(i+j) > 0) deliveryWeight else 0)
            feature(j * 2) = hClicks(i+j)
            feature(j * 2 + 1) = hDeliveries(i+j)
            if (hClicks(i+j) >= clickThreshold || hDeliveries(i+j) > 0) {
              historyActive = true
            }
          }
          var flag = 0.0
          for (j <- 0.until(K)) {
            if (hClicks(i+j+featureLength) >= clickThreshold || hDeliveries(i+j+featureLength) > 0)
              flag = 1.0
          }
          if (historyActive) {
            dataRows = dataRows.:+((row._1, i + featureLength - 1, (flag, Vectors.dense(feature))))
          }
        }
        dataRows
      })

    val test = data
      .flatMap(row => {
        val hClicks = row._2
        val hDeliveries = row._3
        val L = hClicks.length
        var dataRows: List[(String, Int, (Double, Vector))] = List()
        for (i <- (L-featureLength-7).until(L-featureLength-2)) { // L - 8 <= i + fL - 1 < L - 3
          val feature: Array[Double] = Array.fill(featureLength * 2)(0.0)
          var historyActive: Boolean = false
          for (j <- 0.until(featureLength)) {
            //            feature(j) = hClicks(i+j) + (if (hDeliveries(i+j) > 0) deliveryWeight else 0)
            feature(j * 2) = hClicks(i+j)
            feature(j * 2 + 1) = hDeliveries(i+j)
            if (hClicks(i+j) >= clickThreshold || hDeliveries(i+j) > 0) {
              historyActive = true
            }
          }
          var flag = 0.0
          for (j <- 0.until(K)) {
            if (hClicks(i+j+featureLength) >= clickThreshold || hDeliveries(i+j+featureLength) > 0)
              flag = 1.0
          }
          if (historyActive) {
            dataRows = dataRows.:+((row._1, i - (L - featureLength - 7), (flag, Vectors.dense(feature))))
          }
        }
        dataRows
      })

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
            Vectors.dense(features)
          )))
        }
        catch {
          case _: Throwable =>
            (id.toString, (-1, (0.toDouble,
              Vectors.dense(Array.fill(array.size)(0.0))
            )))
        }
      }: (String, (Long, (Double, Vector))))

    val basicData = allData.join(validIds).map {
      case (id, (d, _)) => (id, d._1, d._2)
    }.filter(xs => xs._2 > 0 && xs._2 <= 20180101)
      .map(xs => (xs._1, xs._2.toInt, xs._3))

    println("#BSNSK SIZES: " + training.count().toString + ", " + test.count().toString)
    (training, test)
  }

  def predictionResultLabelsAndScores(
                                          trainingData: RDD[(String, Int, (Double, Vector))],
                                          testData: RDD[(String, Int, (Double, Vector))],
                                          sqlContext: org.apache.spark.sql.SQLContext
  ): RDD[(Int, (Int, Int))] = {

    val numCases = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 50
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10 // ITER_TAG
    val maxBins = 64

    val model = RandomForest.trainClassifier(
      trainingData.map(xs => LabeledPoint(xs._3._1, xs._3._2)),
      numCases, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

//    val numTrees = 10
//    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
//    boostingStrategy.setNumIterations(numTrees)
//    val treeStratery = Strategy.defaultStrategy("Classification")
//    treeStratery.setMaxDepth(8)
//    treeStratery.setNumClasses(2)
//    treeStratery.setCategoricalFeaturesInfo(
//      new java.util.HashMap[java.lang.Integer, java.lang.Integer]
//    )
//    boostingStrategy.setTreeStrategy(treeStratery)
//    val model = GradientBoostedTrees.train(
//      trainingData.map(xs => LabeledPoint(xs._3._1, xs._3._2)),
//      boostingStrategy
//    )

    val baseDate = dataDivideDate
    testData.map(data => {
      val prediction = model.predict(data._3._2)
      (baseDate + data._2, (data._3._1.toInt, prediction.toInt))
    })
  }

  override
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val (training, test) = acquireDividedHistoryData(sc)

    val testLabelsAndScores = predictionResultLabelsAndScores(training, test, sqlContext)

    evalPredictionResult(sc, testLabelsAndScores)
  }
}
