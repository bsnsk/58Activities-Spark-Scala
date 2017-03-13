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
  * Created by Ivan on 2017/3/11.
  */
object PredictorRFLR extends PredictionTest {

  override var identifier: String = "RFLR"
  override var addTimeFeature: Boolean = false

  def predictionResultLabelsAndScores(
                                       trainingData: RDD[(String, Int, (Double, Vector))],
                                       testData: RDD[(String, Int, (Double, Vector))],
                                       sqlContext: org.apache.spark.sql.SQLContext
                                     ): RDD[(Int, (Int, Int))] = {

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

    val newTrainingDataFeed = negativeSamples.sample(false, rate)
      .union(positiveSamples).map(xs => (xs._3._1, xs._3._2))
      .map {
        x => {
          var newFeature = new Array[Double](0)
          for (i<- 0.until(numTrees)) {
            val treePredict = predictModify(rfModel.trees(i).topNode,x._2.toDense)
            val treeArray = new Array[Double]((rfModel.trees(i).numNodes + 1) / 2)
            treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
            newFeature = newFeature ++ treeArray
          }
          (x._1, newFeature)
        }
      }
      .map(x => LabeledPoint(x._1, new DenseVector(x._2)))

    val newTestDataFeed = testData.map(xs => (xs._2, (xs._3._1, xs._3._2)))
      .map {
        xs => {
          val x = xs._2
          var newFeature = new Array[Double](0)
          for (i<- 0.until(numTrees)) {
            val treePredict = predictModify(rfModel.trees(i).topNode,x._2.toDense)
            val treeArray = new Array[Double]((rfModel.trees(i).numNodes + 1) / 2)
            treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
            newFeature = newFeature ++ treeArray
          }
          (xs._1, (x._1, newFeature))
        }
      }
      .map(x => (x._1, LabeledPoint(x._2._1, new DenseVector(x._2._2))))

    //    val model = new LogisticRegressionWithLBFGS()
    //      .setNumClasses(2)
    //      .run(newTrainingDataFeed)
    //      .setThreshold(0.01)

    val model = LogisticRegressionWithSGD.train(
      newTrainingDataFeed,
      10 // number of iterations
    )

    newTestDataFeed
      .map(data => {
        val prediction = model.predict(data._2.features)
        (data._1, (data._2.label.toInt, prediction.toInt))
      })
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
