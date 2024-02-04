import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql
import java.sql.Driver
import org.apache.spark.sql.Encoders


//case class Record(key:Int,value:String)

case class MyData(id: Int, value: String)

object ReadFromHive {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "hdfs://192.168.223.10:9000/usr/hive/warehouse"
    val spark = SparkSession.builder()
      .master(master = "local")
      .appName("ReadFromHive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
//      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
//    System.setProperty("root", "master")

   val SqlData=spark.sql("select cast(id as string) id,name from usr_infor ")
    import spark.implicits._
    val ParsedData=SqlData.map(x=>Array(x.getString(0),x.getString(1)))
    ParsedData.show()

//    spark.sql("set hive.exec.mode.local.auto=true")
//    spark.sql("insert into table default.usr_infor values(237,'phil')")
  }
}
