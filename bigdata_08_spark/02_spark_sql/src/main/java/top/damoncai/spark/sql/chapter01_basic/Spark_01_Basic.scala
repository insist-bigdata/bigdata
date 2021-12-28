package top.damoncai.spark.sql.chapter01_basic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Basic {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")
    // 创建会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转化
    import spark.implicits._

    // DataFrame
    val df: DataFrame = spark.read.json("bigdata_08_spark/datas/json/user.json")

    // DataFrame - SQL
    df.createTempView("user")
    spark.sql("select * from user").show()

    // DataFrame - DSL
    df.select("username").show()
    df.select($"age" + 1).show() // 需要导入隐式转换的包

    // DataSet
    val list = List(1, 2, 3, 4, 5)
    val ds: Dataset[Int] = list.toDS()
    ds.show()

    // RDD <=> DataFrame
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("张三", 3), ("李四", 40)))
    val rddDF: DataFrame = rdd.toDF("username", "age")
    rddDF.show()
    val dfRdd: RDD[Row] = rddDF.rdd

    println("=" * 10 + " RDD <=> DataSet " + "=" * 10)
    // RDD <=> DataSet
    val rddDS: Dataset[User] = rdd.map(item => {
      item match {
        case (username, age) => User(username, age)
      }
    }).toDS()
    rddDS.show()
    val dsRDD: RDD[User] = rddDS.rdd

    println("=" * 10 + " DataFrame <=> DataSet " + "=" * 10)
    // DataFrame <=> DataSet
    val dfDS: Dataset[User] = df.as[User]
    dfDS.show()
    val dsDF: DataFrame = dfDS.toDF()
    dsDF.show()

    //关闭资源
    spark.stop()
  }

  case class User(username:String,age:Long)
}
