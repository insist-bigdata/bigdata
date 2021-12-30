package top.damoncai.spark.sql.chapter06_demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark_02_seach {

  def main(args: Array[String]): Unit = {

    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")

    sparkConf.set("spark.sql.warehouse.dir","hdfs://ha01.prdigital.cn:8020/user/hive/warehouse")
    System.setProperty("HADOOP_USER_NAME", "root")
    // 创建会话
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use atguigu")

    spark.sql(
      """
        |SELECT
        |	c.area,
        |	p.product_name,
        | count(p.product_id) count
        |FROM
        |	user_visit_action a
        |	LEFT JOIN product_info p ON p.product_id = a.click_product_id
        |	LEFT JOIN city_info c ON c.city_id = a.city_id
        |
        |	WHERE a.click_product_id != -1
        |
        |	GROUP BY c.area,p.product_name
        | ORDER BY count DESC
        | LIMIT 3
        |""".stripMargin).show()

    spark.close()
  }
}
