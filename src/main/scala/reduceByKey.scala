import org.apache.spark.{SparkContext,SparkConf}

object reduceByKey {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    var sc = new SparkContext(conf)
    val list = List("hadoop", "spark", "hive", "spark")
    println(list)
    val rdd = sc.parallelize(list)
    val pairRdd = rdd.map((_, 1))
    pairRdd.foreach(println)
//    pairRdd.reduceByKey(_+_).collect.foreach(println)
  }
}
