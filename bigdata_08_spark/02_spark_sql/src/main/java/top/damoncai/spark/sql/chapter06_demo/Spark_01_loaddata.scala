package top.damoncai.spark.sql.chapter06_demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Spark_01_loaddata {

  def main(args: Array[String]): Unit = {

    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")

    sparkConf.set("spark.sql.warehouse.dir","hdfs://ha01.prdigital.cn:8020/user/hive/warehouse")
    System.setProperty("HADOOP_USER_NAME", "root")
    // 创建会话
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("create database atguigu")
    spark.sql("use atguigu")

    // 创建 city_info表
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |`date` string,
        |`user_id` bigint,
        |`session_id` string,
        |`page_id` bigint,
        |`action_time` string,
        |`search_keyword` string,
        |`click_category_id` bigint,
        |`click_product_id` bigint,
        |`order_category_ids` string,
        |`order_product_ids` string,
        |`pay_category_ids` string,
        |`pay_product_ids` string,
        |`city_id` bigint)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)

    // 加载 city_info表数据
    spark.sql(
      """
        |load data local inpath 'bigdata_08_spark/datas/demo/user_visit_action.txt' into table
        |user_visit_action;
        |""".stripMargin)

    // 创建 product_info 表
    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |`product_id` bigint,
        |`product_name` string,
        |`extend_info` string)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)
    // 加载 product_info 表数据
    spark.sql(
      """
        |load data local inpath 'bigdata_08_spark/datas/demo/product_info.txt' into table product_info;
        |""".stripMargin)

    // 创建 city_info 表
    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |`city_id` bigint,
        |`city_name` string,
        |`area` string)
        |row format delimited fields terminated by '\t';
        |""".stripMargin)

    // 加载 city_info 表数据
    spark.sql(
      """
        |load data local inpath 'bigdata_08_spark/datas/demo/city_info.txt' into table city_info;
        |""".stripMargin)

    spark.sql("select * from city_info").show()
    spark.close()
  }
}
