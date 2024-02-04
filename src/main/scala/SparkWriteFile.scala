import org.apache.spark.sql.SparkSession
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.spark.sql
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}


object SparkWriteFile {
  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("writeFile")
      .getOrCreate()

    var rdd = spark.read.format(source = "csv")
      .option("inferschema", "true")
      .option("header", "true")
      .load("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\flight-data\\csv\\2010-summary.csv")
    rdd.show()
//    rdd.write.format("csv")
//      .mode("overwrite")
//      .option("sep", ",")
//      .save("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\output\\res_2010_fighting")
    rdd.write.format("parquet")
      .mode("overwrite")
      .save("C:\\Users\\dell\\Documents\\Spark-The-Definitive-Guide\\data\\output\\other\\res_2010_fighting.parquet")

  }
}
