import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.io.StdIn
import org.apache.spark.sql.functions._

object bigDataTest {
  def main(args: Array[String]): Unit= {
    // 创建SparkSession对象
    val spark = SparkSession.builder()
      .master("local")
      .appName("CreateDataFrame")
      .getOrCreate()

    // 定义Schema信息
    val schema = List("peer_id","id_1","id_2","year")

    // 定义数据集合
    val data = Seq(
      ("AE686(AE)", "7", "AE686", 2022),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2023),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OA691", 2022),
      ("AE686(AE)", "12", "OB691", 2022),
      ("AE686(AE)", "12", "OC691", 2019),
      ("AE686(AE)", "12", "OD691", 2017)
    )


//    // 将数据转换成RDD并应用schema
//    val rdd = spark.sparkContext.parallelize(data)


    val df=spark.createDataFrame(data).toDF(schema:_*)

    df.show()
    df.createTempView("test")
    //step.1
    val res1=spark.sql("select peer_id,year from test where peer_id like concat('%',id_2,'%') ")
    res1.createTempView("test1")
    println(s"the result of step 1 is")
    res1.show()

    //step.2
    println(s"Given a size number:")
    val num=StdIn.readLine().toInt

    println(s"you given the number is:"+num)

    println(s"the result of step 2 is:")
    val res2=spark.sql("select a.peer_id,a.year,count(a.peer_id) as cnt " +
      "from test a " +
      "join test1 b on b.peer_id=a.peer_id " +
      "where a.year<=b.year " +
      "group by a.peer_id,a.year order by peer_id,year desc")

    res2.createTempView("test2")

    res2.selectExpr("peer_id","year").show()

    //step.3
  val mid=spark.sql("select peer_id,year,cnt,sum(cnt) over (partition by peer_id order by peer_id,year desc) acc_cnt " +
    "from test2 " ).toDF()

    mid.createTempView("test3")

    // Define a window specification to partition by "peer_id" and order by "year"
    val windowSpec = Window.partitionBy("peer_id").orderBy(col("year").desc)

    //cnt 是以year为分区的peer_id的记录数求和，acc_cnt 为peer_id 累加数
    val mid1 = mid.withColumn("flag", //add one field,Identify the records that meet the conditions
      when(row_number().over(windowSpec) === 1 && col("acc_cnt") >= num , 1) //case when peer_id has one record,and acc_cnt>=given number
        .when(row_number().over(windowSpec) === 1 && col("acc_cnt") < num ,1) //case when peer_id has one record,and acc_cnt<given number
        .when(row_number().over(windowSpec) >1 && col("acc_cnt") < num ,1) //case when peer_id has one more record,and acc_cnt<given number
        .when(row_number().over(windowSpec) >1 && col("acc_cnt") >= num && col("acc_cnt")-col("cnt")<num ,1) //The cumulative value of the previous record is less than the given value
        .when(row_number().over(windowSpec) >1 && col("acc_cnt") >= num && col("acc_cnt")-col("cnt")>=num ,0).otherwise(0) //The cumulative value of the previous record is greater than or equal to the given value
    )

//    mid1.filter(col("flag")===1).show()
//      mid1.show()
    //result of step3
    val mid2=mid1.selectExpr("peer_id","year").filter(col("flag")===1).orderBy("year")
    println(s"The final expected output for the given number is")
//    res3.collect().map(row=>row.mkString(",")).foreach(println)

    val res3=mid2.rdd.map(row => (row.getString(0), row.getInt(1))).collect().toList
    res3.foreach(println)
    spark.stop()
  }
}
