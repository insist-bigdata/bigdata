package top.damoncai.spark.sql.chapter02_udf

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark_01_udf {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")
    // 创建会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转化
    import spark.implicits._

    // 注册UDF
    spark.udf.register("prefix",(arg1:String,arg2:Long) => "prefix" + arg1 + arg2)

    // DataFrame
    val df: DataFrame = spark.read.json("bigdata_08_spark/datas/json/user.json")

    // DataFrame - SQL
    df.createTempView("user")
    spark.sql("select prefix(username,age),age from user").show()

    //关闭资源
    spark.stop()
  }

}
