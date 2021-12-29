package top.damoncai.spark.sql.chapter04_mysql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object Spark_02_mysql_write {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")
    // 创建会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val rdd: RDD[User2] = spark.sparkContext.makeRDD(List(User2("lisi", 20),
      User2("zs", 30)))
    val ds: Dataset[User2] = rdd.toDS

    //方式 1：通用的方式 format 指定写出类型
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("user", "root")
      .option("password", "zhishun.cai")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()
    //方式 2：通过 jdbc 方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "zhishun.cai")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://linux1:3306/spark-sql",
      "user", props)

  }

  case class User2(name: String, age: Long)

}
