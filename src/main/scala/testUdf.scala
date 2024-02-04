import org.apache.spark.sql.SparkSession

//object DataFrameExample extends Serializable{
//  def main(args: Array[String])={
//    val pathToDataFolder=args(0)
//    //显示配置
//    val spark=SparkSession.builder().appName("Spark Example")
//      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
//      .getOrCreate()
//    //注册用户自定义函数
//    spark.udf.register("myUDF",someUDF(_:String):String)
//    val df=spark.read.json(pathToDataFolder+"2010-summary.json")
//    val manipulated =df.groupBy().sum().collect().foreach(x=>println(x))
//  }
//}



//def sumPowersOfTwo(a: Int, b: Int): Int = {
//  if (a > b) 0 else powerOfTwo(a) + sumPowersOfTwo(a + 1, b)
//}



object udf{

  def main(args: Array[String]) = {

    def powerOfTwo(x: Int): Int = {
      if (x == 0) 1 else 2 * powerOfTwo(x - 1)
    }

//    def sumPowersOfTwo(a: Int, b: Int): Int = {
//      if (a > b) 0 else powerOfTwo(a) + sumPowersOfTwo(a + 1, b)
//    }
//    def sumPowersOfTwo(a: Int, b: Int):Int= sum(powerOfTwo,a,b)

    def sum(f: Int => Int, a: Int, b: Int): Int = {
      if (a > b) 0 else f(a) + sum(f, a + 1, b)
    }
    def sumPowersOfTwo(a: Int, b: Int): Int = sum(powerOfTwo, a, b)

    println(powerOfTwo(3))

    println(sumPowersOfTwo(1,3))

  }


}
