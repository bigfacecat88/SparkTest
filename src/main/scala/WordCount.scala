import org.apache.commons.io.file.PathUtils.deleteDirectory
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.{Files, Paths}

object WordCount {
  //  def main(args:Array[String]):Unit={
  //    val spark = SparkSession.builder()
  //      .master("local")
  //      .getOrCreate()

  //val rdd = spark.read.format("csv")
  //  .option("header", "true")
  //  .option("inferschema", "true")
  //  .load("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\jd.txt")
  //rdd.createTempView("dfTable")
  //}
  def main(args: Array[String]): Unit = {
    var context = new SparkConf().setAppName("WordCount").setMaster("local")
    var sc = new SparkContext(context)
    var input = sc.textFile("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\jd.txt")
    var res1 = input.flatMap(line => line.split(" ")) //根据分隔符拆分
    var res2 = res1.filter(s => !s.contains("•"))
    var res3 = res2.map(res1 => (res1, 1)).reduceByKey(_ + _) //分组聚合
    res3.foreach(println)
    val directory = Paths.get("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\output\\res_WordCount")
    if (Files.exists(directory)) {
      println("文件存在，准备删除...")
      deleteDirectory(directory)
      println("文件删除成功.")
    } else {
      println("文件不存在.")
    }
    res3.saveAsTextFile("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\output\\res_WordCount")
    sc.stop()
    //contains("•")
  }
}

