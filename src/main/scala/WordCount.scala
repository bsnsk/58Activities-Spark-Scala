import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bsnsk on 2016/12/2.
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkFeatureExtractor")
    val sc = new SparkContext(conf)
    val fileName = "/Users/Ivan/proj/shadowsocks-libev/README.md"
    val textFile = sc.textFile(fileName)
    val counts = textFile
      .flatMap(line => line.toString().split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(x => x.swap, ascending = false)
    counts.saveAsTextFile("wordCountResult")
    sc.stop()
  }
}
