package top.damoncai.spark.sql.chapter05_hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Spark_01_hive {

  def main(args: Array[String]): Unit = {
    //创建 SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("sql")
      .getOrCreate()

    spark.sql("show databases").show();

    spark.close()
  }
}
