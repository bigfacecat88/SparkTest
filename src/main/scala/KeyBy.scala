import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf



object KeyBy {
  def main(args:Array[String]):Unit={

    val conf = new SparkConf().setAppName("test KeyBy").setMaster("local")
    val spark = new SparkContext(conf)
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.parallelize(myCollection, 2)
//    words.foreach(println)
//    words.map(word=>(word.toLowerCase,1))
    val keyword=words.keyBy(word=>word.toLowerCase.toSeq(0).toString)

    keyword.mapValues(word=>word.toUpperCase).collect().foreach(println)



  }

}
