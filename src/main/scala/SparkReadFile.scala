import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession


object SparkReadFile {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local")
      .getOrCreate()

    val test = spark.read.format("csv")
      .option("header", "true")
      .option("inferschema", "true")
      .load("D:/student.txt")

    test.show()
    println(test.dtypes)
    test.printSchema()
  }

}
