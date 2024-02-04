import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AccTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test accu").getOrCreate()
    val LongAccumulator=spark.sparkContext.longAccumulator("Long Accumulator")
    val data=List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd=spark.sparkContext.parallelize(data)

    //    rdd.collect.foreach(println)
    def count_negatives(x: Int): Unit = {
      if (x%2==0) {
        LongAccumulator.add(1)
      }
    }

  rdd.foreach(count_negatives)
    println(s"Number of negative numbers: ${LongAccumulator.value}")






  }
}
