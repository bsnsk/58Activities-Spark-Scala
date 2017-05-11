import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

/**
  * Created by Ivan on 10/04/2017.
  * Experimental! Not tested yet!
  */
object ResumeKatzIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkResumeSim")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val deliveryWeight = 5.0

    val data = sc.textFile("hdfs:///user/shuyagshi/58data_resumeClickDeliveryPairs")
      .map(_.split("\001"))
      .map(xs => try {
        val rid = xs(0).toLong
        val pid = xs(1).toLong
        val t = if (xs(3) == "C") 1.0 else deliveryWeight
        (rid, pid, t)
      } catch {
        case _: Throwable => (-1, -1, 0.0)
      })
      .filter(xs => xs._1 != -1)
      .map(xs => (xs._1.toString, xs._2.toString, xs._3))
      .toDF("resume", "position", "relation")


    val resumeIndexer = new StringIndexer().
      setInputCol("resume").
      setOutputCol("resumeInt").
      fit(data)

    val positionIndexer = new StringIndexer().
      setInputCol("position").
      setOutputCol("positionInt").
      fit(data)

    val resumeIndexConverter = new IndexToString().
      setInputCol("id").
      setOutputCol("resumeId").
      setLabels(resumeIndexer.labels)

    val transformedData = resumeIndexer.transform(positionIndexer.transform(data))

    val entries = transformedData.rdd.map(xs =>
      MatrixEntry(xs(0).toString.toLong, xs(1).toString.toLong, xs(2).toString.toDouble
    ))

    var mat = new CoordinateMatrix(entries).toBlockMatrix()

    val numIterations = 3
    for (_ <- 0.until(numIterations)) {
      mat = mat.multiply(mat)
    }

    val matT = mat.transpose.toIndexedRowMatrix()
    val katz = matT.rows.map(xs => xs.vector.toArray.sum).collect()
    var lst:List[(Int, Double)] = List()
    for (i <- katz.indices) {
      lst = lst.:+((i, katz(i)))
    }
    val df = resumeIndexConverter.transform(lst.toDF("id", "katz"))

    val outputPath = "hdfs:///user/shuyangshi/58feature_resumekatz"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputPath), true)
    df
      .rdd
      .map(xs => xs(0).toString + " " + xs(1).toString)
      .saveAsTextFile(outputPath)

  }

}
