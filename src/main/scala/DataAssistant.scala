import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 17/05/2017.
  */
object DataAssistant {

  def prepareData(tagString: String, sc: SparkContext):
  RDD[(String, Int, (Double, Vector))] = {
    val result = prepareDataImple(tagString, sc)
    val numResumes = result.map(_._1).distinct().count().toInt
//    val numDataPerDay = result.map(xs => (xs._2, 1)).reduceByKey(_+_).sortByKey().collect()
    val numPositive = result.map(_._3._1).filter(_ > 0.5).count
    val numNegative = result.map(_._3._1).filter(_ < 0.5).count
    println("# BSNSK - # Resumes = " + numResumes.toString)
    println("# BSNSK - # Pos / Neg = " + numPositive.toString + ", " + numNegative.toString)
//    println("# BSNSK - # Data / Day = " + numDataPerDay.toSeq.toString())
    result
  }

  def prepareDataImple(tagString: String, sc: SparkContext):
      RDD[(String, Int, (Double, Vector))]
    = {

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

    val data = allData.join(validIds).map {
      case (id, (d, _)) => (id, d._1, d._2)
    }.filter(xs => xs._2 > 0 && xs._2 <= 20180101)
      .map(xs => (xs._1, xs._2.toInt, xs._3))

    if (tagString == "B") {
      data.map(xs => (xs._1, xs._2, (xs._3._1, Vectors.dense(xs._3._2.toArray.slice(0, 12)))))
    }
    else if (tagString == "BM") {
      data.map(xs => (xs._1, xs._2, xs._3))
    }
    else if (tagString == "M"){
      data.map(xs => {
        val arr = xs._3._2.toArray
        (xs._1, xs._2, (xs._3._1, Vectors.dense(arr.slice(12, arr.length))))
      })
    }
    else if (tagString == "BH") {
      val textFile = sc.textFile("hdfs:///user/shuyangshi/58feature_history")
      val historyData = textFile.
        map(row => {
          val resumeId = row.split(",")(0).substring(1)
          val historyClicks = row.split("WrappedArray")(1).
            substring(1).split(')')(0).split(',').map(_.toDouble)
          val historyDeliveries = row.split("WrappedArray")(2).
            substring(1).split(')')(0).split(',').map(_.toDouble)
          (resumeId, (historyClicks, historyDeliveries))
        })

      val K = PredictorLRWithHis.KHistoryLength

      val dates = data.map(_._2).distinct()
        .filter(_>=20160101).filter(_<=20180101)
        .collect().sorted

      PredictorLRWithHis.appendHistoryDataToRDD(
        data.map(xs => (xs._1, xs._2, (xs._3._1, Vectors.dense(xs._3._2.toArray.slice(0, 12))))),
        historyData,
        dates,
        K
      )
    }
    else if (tagString == "HM" || tagString == "MH") {
      val textFile = sc.textFile("hdfs:///user/shuyangshi/58feature_history")
      val historyData = textFile.
        map(row => {
          val resumeId = row.split(",")(0).substring(1)
          val historyClicks = row.split("WrappedArray")(1).
            substring(1).split(')')(0).split(',').map(_.toDouble)
          val historyDeliveries = row.split("WrappedArray")(2).
            substring(1).split(')')(0).split(',').map(_.toDouble)
          (resumeId, (historyClicks, historyDeliveries))
        })

      val K = PredictorLRWithHis.KHistoryLength

      val dates = data.map(_._2).distinct()
        .filter(_>=20160101).filter(_<=20180101)
        .collect().sorted

      PredictorLRWithHis.appendHistoryDataToRDD(
        data.map(xs => {
          val arr = xs._3._2.toArray
          (xs._1, xs._2, (xs._3._1, Vectors.dense(arr.slice(13, arr.length))))
        }),
        historyData,
        dates,
        K
      )
    }
    else if (tagString == "BHM" || tagString == "BMH") {
      val textFile = sc.textFile("hdfs:///user/shuyangshi/58feature_history")
      val historyData = textFile.
        map(row => {
          val resumeId = row.split(",")(0).substring(1)
          val historyClicks = row.split("WrappedArray")(1).
            substring(1).split(')')(0).split(',').map(_.toDouble)
          val historyDeliveries = row.split("WrappedArray")(2).
            substring(1).split(')')(0).split(',').map(_.toDouble)
          (resumeId, (historyClicks, historyDeliveries))
        })

      val K = PredictorLRWithHis.KHistoryLength

      val dates = data.map(_._2).distinct()
        .filter(_>=20160101).filter(_<=20180101)
        .collect().sorted

      PredictorLRWithHis.appendHistoryDataToRDD(
        data,
        historyData,
        dates,
        K
      )
    }
    else {
      println("ERROR: Wrong tag string: " + tagString)
      data.filter(_ => false)
    }

  }




}
