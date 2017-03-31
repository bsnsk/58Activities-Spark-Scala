import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.optimization.{Gradient, GradientDescent, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType, Strategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Ivan on 2017/3/22.
  */
object PredictorRFMatLR extends PredictionTest {

  override var identifier: String = "RFMatLR"
  override var addTimeFeature: Boolean = true

  class myGradient extends Gradient {
    override def compute(data: Vector, label: Double, weights: Vector, cumGradient: Vector): Double = {

      val dataArray = data.toArray
      val numFeatures = data.size / 4
      val dataCurrent = dataArray.slice(0, numFeatures)
      val relatedResumes = List(
        dataArray.slice(numFeatures, numFeatures * 2),
        dataArray.slice(numFeatures * 2, numFeatures * 3),
        dataArray.slice(numFeatures * 3, numFeatures * 4)
      )

      val margin = -1.0 * (dataCurrent, weights.toArray).zipped.map(_*_).sum
      val multiplier = (1.0 / (1.0 + math.exp(margin))) - label

      val dateParameter = 0.1
      //      val dateMultiplier = math.exp(- dateParameter * (maxDayOfYear - data(numFeatures - 1)))
      val dateMultiplier = 1

      // gradient update

      val matchParam = 0.1
      val extraLoss: Array[Double] = Array()
      val square: Double => Double = x => x * x
      val dotMult: Array[Double] => Double = d =>
        (d, weights.toArray).zipped.map(_*_).sum
      var accumulativeGradient = cumGradient.toDense.values
      for (i <- relatedResumes.indices) {
        val dataRelated = relatedResumes(i)
        for (k <- 0.until(numFeatures-1)) {
          for (r <- 0.until(numFeatures-1)) {
            accumulativeGradient(k) += 2 * matchParam *
              (dataRelated(k) - dataCurrent(k)) *
              (dataRelated(r) - dataCurrent(r)) *
              weights(r)
          }
        }
        square(dotMult(dataCurrent) - dotMult(dataRelated)) * matchParam +: extraLoss
      }

      for (i <- dataCurrent.indices) {
        val feature = dataCurrent(i)
        if (i < numFeatures - 1) {
          accumulativeGradient(i) += multiplier * feature * dateMultiplier
        }
      }

      // loss
      if (label > 0.5) {
        dateMultiplier * math.log(1 + math.exp(margin)) + extraLoss.sum
      } else {
        dateMultiplier * (math.log(1 + math.exp(margin)) - margin) + extraLoss.sum
      }

    }
  }

  def treeFeatures(
                    trainingData: RDD[(String, Int, (Double, Vector))],
                    testData: RDD[(String, Int, (Double, Vector))]
                  ): (RDD[(String, Int, (Double, Vector))], RDD[(String, Int, (Double, Vector))]) = {

    val positiveSamples = trainingData.filter(r => r._3._1 == 1)
    val negativeSamples = trainingData.filter(r => r._3._1 == 0)
    val rate = positiveSamples.count().toDouble / negativeSamples.count().toDouble
    val trainingDataFeed = negativeSamples.sample(false, rate)
      .union(positiveSamples).map(xs => LabeledPoint(xs._3._1, xs._3._2))

    val numTrees = 50
    val rfModel = RandomForest.trainClassifier(
      trainingDataFeed,
      numClasses = 2,
      Map[Int, Int](),
      numTrees = numTrees,
      featureSubsetStrategy = "auto",
      impurity = "gini",
      maxDepth = 10,
      maxBins = 64,
      seed = 32793
    )

    val treeLeafArray = new Array[Array[Int]](numTrees)
    for(i<- 0.until(numTrees)){
      treeLeafArray(i) = getLeafNodes(rfModel.trees(i).topNode)
    }

    val newTrainingData = negativeSamples.sample(false, rate)
      .union(positiveSamples)
      .map {
        xs => {
          val x = (xs._3._1, xs._3._2)
          var newFeature = new Array[Double](0)
          for (i<- 0.until(numTrees)) {
            val treePredict = predictModify(rfModel.trees(i).topNode,x._2.toDense)
            val treeArray = new Array[Double]((rfModel.trees(i).numNodes + 1) / 2)
            treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
            newFeature = newFeature ++ treeArray
          }
          (xs._1, xs._2, x._1, newFeature)
        }
      }
      .map(xs => (xs._1, xs._2, (xs._3, Vectors.dense(xs._4))))

    val newTestDataFeed = testData//.map(xs => (xs._2, (xs._3._1, xs._3._2)))
      .map {
        xs => {
          val x = xs._3
          var newFeature = new Array[Double](0)
          for (i<- 0.until(numTrees)) {
            val treePredict = predictModify(rfModel.trees(i).topNode,x._2.toDense)
            val treeArray = new Array[Double]((rfModel.trees(i).numNodes + 1) / 2)
            treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
            newFeature = newFeature ++ treeArray
          }
          (xs._1, xs._2, (x._1, Vectors.dense(newFeature)))
        }
      }
    (newTrainingData, newTestDataFeed)
  }

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

    val convergenceTol = 1e-4
    val numIterations = 10
    val regParam = 0.01
    val nF = trainingData.map(_._3._2.size).max() / 4 - 1
    numberOfFeatures = nF
    val initialWeights = Vectors.dense(Array.fill(nF)(0.0))

    println("BSNSK #Feature " + (trainingData.first()._3._2.size - 1).toString + " " + numberOfFeatures.toString)
    println("BSNSK MaxDayOfYear " + maxDayOfYear.toString)
    maxDayOfYear = trainingData.map(r =>
      r._3._2(nF)
    ).max().toInt
    println("BSNSK MaxDayOfYear (updated) " + maxDayOfYear.toString)

    val gradient = new myGradient()

//    val trainingDataFeed = negativeSamples.sample(false, rate)
//      .union(positiveSamples).map(_._3)


    val weights = GradientDescent
      .runMiniBatchSGD(
        trainingData.map(_._3),
        gradient,
        new SquaredL2Updater(),
        stepSize = 0.1,
        numIterations,
        regParam,
        miniBatchFraction = 1.0,
        initialWeights,
        convergenceTol
      )._1

    val calcScore: Vector => Double = features => 1.0 / (1.0 + scala.math.exp(
      - (weights.toArray, features.toArray).zipped.map(_*_).sum
    ))

    testData
      .map(data => {
        val prediction = if (calcScore(data._3._2) > 0.5) 1 else 0
        (data._2, (data._3._1.toInt, prediction.toInt))
      })

  }

  def addMatchedResumes(
                         sc: SparkContext,
                         training: RDD[(String, Int, (Double, Vector))]
                       )
  : RDD[(String, Int, (Double, Vector))] = {

    val textFiles = sc.textFile("hdfs:///user/shuyangshi/58data_similarResumes/similarResumeData/part-*")
    val matchIndex = textFiles.map(_.split("\001")).map(xs => ((xs(0), xs(1).toInt), xs.drop(2)))
    val toBeJoined = training.map(xs => ((xs._1, xs._2), xs._3._2.toArray))
    val results = training.map(xs => ((xs._1, xs._2), xs._3))
      .join(matchIndex)
      .map {
        case (key, (vector, matches)) => ((matches(0), key._2), (key, vector, matches))
      }
      .leftOuterJoin(toBeJoined)
      .map {
        case (_, (data, matchedRow)) => ((data._3(1), data._1._2),
          ( data._1,
            ( data._2._1,
              if (matchedRow.isDefined) data._2._2.toArray ++ matchedRow.get else data._2._2.toArray
            ),
            data._3
          )
        )
      }
      .leftOuterJoin(toBeJoined)
      .map {
        case (_, (data, matchedRow)) => ((data._3(2), data._1._2),
          ( data._1,
            ( data._2._1,
              if (matchedRow.isDefined) data._2._2 ++ matchedRow.get else data._2._2
            )
          )
        )
      }
      .leftOuterJoin(toBeJoined)
      .map {
        case (_, (data, matchedRow)) => (
          data._1._1, // resume id : String
          data._1._2, // date : Int
          (data._2._1, Vectors.dense((
            if (matchedRow.isDefined) data._2._2 ++ matchedRow.get else data._2._2
            ).map(_.toString.toDouble))
          )
        )
      }

    println("## BSNSK CNTJOINED = " + results.count().toString + " / " + training.count().toString)
    results
  }

  override
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val (training, test) = acquireDividedData(sc)
    val (updatedTraining, updatedTest) = treeFeatures(training, test)
    val latestTraining = addMatchedResumes(sc, updatedTraining)

    val testLabelsAndScores = predictionResultLabelsAndScores(latestTraining, updatedTest, sqlContext)

    evalPredictionResult(sc, testLabelsAndScores)
  }

  //get decision tree leaf's nodes
  def getLeafNodes(node:Node):Array[Int] = {
    var treeLeafNodes = new Array[Int](0)
    if (node.isLeaf){
      treeLeafNodes = treeLeafNodes.:+(node.id)
    }else{
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.leftNode.get)
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.rightNode.get)
    }
    treeLeafNodes
  }

  // predict decision tree leaf's node value
  def predictModify(node:Node,features:DenseVector):Int={
    val split = node.split
    if (node.isLeaf) {
      node.id
    } else {
      //      if (split.get.featureType == FeatureType.Continuous) {
      if (features(split.get.feature) <= split.get.threshold) {
        //          println("Continuous left node")
        predictModify(node.leftNode.get,features)
      } else {
        //          println("Continuous right node")
        predictModify(node.rightNode.get,features)
      }
      //      } else {
      //        if (split.get.categories.contains(features(split.get.feature))) {
      //          //          println("Categorical left node")
      //          predictModify(node.leftNode.get,features)
      //        } else {
      //          //          println("Categorical right node")
      //          predictModify(node.rightNode.get,features)
      //        }
      //      }
    }
  }
}
