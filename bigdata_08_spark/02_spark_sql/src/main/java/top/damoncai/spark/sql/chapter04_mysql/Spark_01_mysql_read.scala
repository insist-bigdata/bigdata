package top.damoncai.spark.sql.chapter04_mysql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties

object Spark_01_mysql_read {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")
    // 创建会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //方式 1：通用的 load 方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "zhishun.cai")
      .option("dbtable", "user")
      .load().show

    //方式 2:通用的 load 方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://linux1:3306/spark-sql?user=root&password=zhishun.cai",
        "dbtable"->"user",
        "driver"->"com.mysql.jdbc.Driver"))
      .load().show

    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "zhishun.cai")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://linux1:3306/spark-sql", "user", props)
    df.show

  }
}
