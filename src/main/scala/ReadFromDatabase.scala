import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ReadFromDatabase {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder()
      .master("local")
      .appName("loadFromDatabase")
      .config("spark.driver.extraClassPath", "D:/spark/jars/ojdbc8.jar")
      .config("spark.executor.extraClassPath", "D:/spark/jars/ojdbc8.jar")
      .config("spark.jars", "D:/spark/jars/ojdbc8.jar")
      //.config("spark.driver.user", "scott")
      //.config("spark.driver.password", "tiger")
      .getOrCreate()

    val data = spark.read.format("jdbc")
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("url", "jdbc:oracle:thin:@127.0.0.1:1521:orcl")
      .option("user","scott")
      .option("password","tiger")
      .option("dbtable","hr.jobs")
      //.option("numPartitions", 20)
      .load()

//    data.createTempView("test")
//    spark.sql("insert into hr.senior_jobs select * from test where MAX_SALARY>10000")
//    println(data.dtypes)
    var data1=data.filter("MAX_SALARY >10000")
    data1.show()
    data1.write.format(source = "jdbc")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("url", "jdbc:oracle:thin:@127.0.0.1:1521:orcl")
      .option("user", "scott")
      .option("password", "tiger")
      .option("dbtable", "hr.senior_jobs")
      .mode("append")
      .save()
//    var data2=data1.collect() //聚合为一列
//    data2.foreach(row=>
//    {val SALARY=row.get(3)
//      println(SALARY)
//    })

//    var df=data.toDF()
//    var df1=df.filter("MAX_SALARY>10000")
//    df.printSchema()
    spark.stop()
  }

}

