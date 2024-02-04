import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext._


//case class Fight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)

object ReadFromJson {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .master("local")
      .appName("read data from json")
      .getOrCreate()
    var sc=spark.read.format("json")
      .option("inferSchema","true")
      .option("mode","FAILFAST")
      .load("C:/Users/dell/Documents/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json")
    var sc1=sc.filter("DEST_COUNTRY_NAME='United States'")
    import spark.implicits._

//    val fight=sc.toDF("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME","count")
    //val sc1=sc.as[(String, String, Double)]


//    var fights=sc.as[Fight]
//    var res=fights.groupBy("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").sum("count")
      var res= sc1.map(x=>x.getString(0)).collect().toList

      var res1=spark.sparkContext.parallelize(res,2)


//    var res1=spark.sparkContext.parallelize(res)
      var res2=res1.map((_, 1)).reduceByKey(_ + _)
      res2.foreach(println)

//    res2.reduceByKey(_+_).collect().foreach(println)











}
//  .map(x=>{(x.getString(1)+x.getString(2))+x.getString(3)]})

//        .groupByKey("DEST_COUNTRY_NAME","count")


}







//    sqlContext.sql("create or replace table hr.fighting(DEST_COUNTRY_NAME varchar2(50),ORIGIN_COUNTRY_NAME varchar2(50),count number(10));")
//    spark.sql("create or replace table hr.fighting(DEST_COUNTRY_NAME varchar2(50),ORIGIN_COUNTRY_NAME varchar2(50),count number(10));")
//    spark.stop()

