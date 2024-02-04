import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

object SparkStreaming {
  def main(args: Array[String]): Unit= {
    //  val conf = new SparkConf().setAppName("TestStreaming").setMaster("local[2]")
    //  val ssc = new StreamingContext(conf, Seconds(2))
    //  val lines=ssc.textFileStream("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day")
    //    lines.print()
    //    ssc.start()

    //创建SPARK会话
    val spark = SparkSession.builder
      .appName("SparkStreaming")
      .master("local")
      .config("spark.executor.heartbeatInterval", "15000ms")
      .getOrCreate()

    spark.conf.set("spark.sql.hive.filesourcePartitionFileCacheSize", "314572800")
    //    spark.sql.shuffle.partitions=100
    import spark.implicits._
    val schema = new StructType()
      .add("InvoiceNo", StringType)
      .add("StockCode", StringType)
      .add("Description", StringType)
      .add("Quantity", LongType)
      .add("InvoiceDate", StringType)
      .add("UnitPrice", LongType)
      .add("CustomerID", StringType)
      .add("Country", StringType)

    //通过spark.readstream 读取目录中CSV文件数据
    var csv_stream_df = spark.readStream
      .option("header", "true")
      .schema(schema)
      .option("maxFilesPerTrigger", 5) // 设置每次触发器最大处理的文件数
      .csv("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day")

//    val currentPartitions = csv_stream_df.rdd.getNumPartitions
//    println(s"Current number of partitions: $currentPartitions")

    //创建HiveWarehouseSession 会话，准备写入HIVE
    val warehouseLocation = "hdfs://192.168.223.10:8020/user/hive/warehouse"
    val spark2hive = SparkSession.builder()
      .master("local")
      .config("spark.executor.heartbeatInterval", "15000ms")
      .appName("Write to Hive using Spark Structured Streaming")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark2hive.conf.set("spark.sql.hive.filesourcePartitionFileCacheSize", "314572800")

    //判断HDFS存储路径中 文件是否存在 存在就清除
    // 设置 HDFS 路径
//        val hdfsPath = "/user/hive/warehouse//retail_data"
    //    val hdfstmp="hdfs://192.168.223.10:9000/tmp/checkpoint"
    val iter = Iterator("/user/hive/warehouse/retail_data", "/tmp/checkpoint")

    // 获取 Hadoop 配置
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://192.168.223.10:8020");

    // 获取 Hadoop FileSystem 对象
    val fs = FileSystem.get(conf)

    // 判断路径是否存在
    for (elem <- iter) {
      if (fs.exists(new Path(elem))) {
        // 如果存在，清空目录
        fs.delete(new Path(elem), true)
        println(s"Path $elem exists and has been cleared.")
      } else {
        println(s"Path $elem does not exist.")
      }
    }

    //    val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark2hive).build()
    // 定义流式查询并将结果写入 Hive 表
    import spark2hive.implicits._
//    csv_stream_df.repartition($"partitionColumn", numPartitions)
    var query = csv_stream_df.writeStream
      .format("hive")
      .outputMode("append")
      .option("maxFilesPerTrigger", 5)  // 设置每次触发器最大处理的文件数
      .option("checkpointLocation", "hdfs://192.168.223.10:8020/tmp/checkpoint") // 检查点目录
//      .partitionBy("invoicedate","country")
      .option("path","hdfs://192.168.223.10:8020/usr/hive/warehouse")
      .trigger(Trigger.ProcessingTime("5 seconds")) // 触发器，定义处理时间间隔
      .foreachBatch { (batchDF: DataFrame, batchId: Long) => batchDF.write.mode("append")
        .saveAsTable("retail_data") }
      // 在这里可以进行每个微批次的额外处理
      .start()
    // 等待流式查询完成
    query.awaitTermination()


    //*不断读取CSV 分组更新打印到终端显示
//    val count=csv_stream_df.groupBy("Country").count()
//    spark.conf.set("spark.sql.shuffle.partitions",5) // 本地模式 防止shuffle过多
//    val CountryQuery=count.writeStream
//      .queryName("countryCount")
//      .format("console").outputMode("complete")
//      //outputMode:1 append 2 complete 3 update
//      .trigger(Trigger.ProcessingTime(200))
//      .queryName("query_country")
//      .start()
//       CountryQuery.awaitTermination()

//    var query=csv_stream_df.writeStream
//      .outputMode("append")
////      .trigger(Trigger.ProcessingTime(200))
//      .format("console")
//      .start()

//    spark.stop()



  }
}
