import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 27/03/2017.
  */
object ResumeMatFact {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkResumeSim")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val textFile = sc.textFile("hdfs:///user/shuyangshi/58data+resumeClickDeliveryPairsDedup/part*")

    val deliveryWeight = 5.0

    val records = textFile.map(row => {
      val data = row.split("\001")
      ((data(0), data(1)), if (data(3) == "C") 1.0 else deliveryWeight)
    })
      .reduceByKey(_+_)
      .map(xs => (xs._1._1, (xs._1._2, xs._2)))

//    val validResumes = records.map(xs => (xs._1, 1)).reduceByKey(_+_).filter(_._2>=3)
//    val validRecords = records.join(validResumes)
//      .map(xs => (xs._1, xs._2._1._1, xs._2._1._2))
//      .toDF("resume", "position", "activity")

    val validRecords = records
      .map(xs => (xs._1, xs._2._1, xs._2._1))
      .toDF("resume", "position", "activity")

    val resumeIndexer = new StringIndexer().
      setInputCol("resume").
      setOutputCol("resumeInt").
      fit(validRecords)

    val positionIndexer = new StringIndexer().
      setInputCol("position").
      setOutputCol("positionInt").
      fit(validRecords)

    val resumeIndexConverter = new IndexToString().
      setInputCol("id").
      setOutputCol("resumeId").
      setLabels(resumeIndexer.labels)

    val als = new ALS()
      .setRank(50)
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("resumeInt")
      .setItemCol("positionInt")
      .setRatingCol("activity")
      .fit(
        resumeIndexer.transform(positionIndexer.transform(validRecords))
      )

    val resumeVectors = resumeIndexConverter.transform(als.userFactors).select("resumeId", "features")

    val outputPath = "hdfs:///user/shuyangshi/58feature_resumevectors"
    val outputPathTextFile = "hdfs:///user/shuyangshi/58feature_resumevectors_text"

    resumeVectors.write.mode("overwrite").save(outputPath)

    val labeledData = sc.textFile("hdfs:///user/shuyangshi/58data_labeledNoSQL/part-*")

    val allData = labeledData
      .map(r => {
        val id = r.split(',')(0).split('[')(1)
        val date = r.split(',')(1)
        try {
          (id.toString, date.toLong)
        }
        catch {
          case _: Throwable =>
            (id.toString, -1)
        }
      }: (String, Long))

    val data = allData

    val dividerDate = PredictorHistory.dataDivideDate
    val testResumes = data.filter(pair => pair._2 >= dividerDate && pair._2 > 0 && pair._2 <= 20161010)
      .map(r => (r._1, 1))

    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    resumeVectors
      .rdd
      .map(row => (row(0).toString, (row(1), row(2))))
      .join(testResumes).map {
        case (resumeId, (history, _)) => (resumeId, history._1, history._2)
      }
      .repartition(1)
      .saveAsTextFile(outputPathTextFile)
  }
}
