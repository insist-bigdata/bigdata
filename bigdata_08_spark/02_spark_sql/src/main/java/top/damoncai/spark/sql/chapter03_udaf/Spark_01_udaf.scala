package top.damoncai.spark.sql.chapter03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark_01_udaf {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")
    // 创建会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转化

    // 注册UDF
    spark.udf.register("avgAge", new CustomAvgAge())

    // DataFrame
    val df: DataFrame = spark.read.json("bigdata_08_spark/datas/json/user.json")

    // DataFrame - SQL
    df.createTempView("user")
    spark.sql("select avgAge(age) from user").show()

    //关闭资源
    spark.stop()
  }

  class CustomAvgAge() extends UserDefinedAggregateFunction{

    /**
     * 输入的数据类型 In
     * @return
     */
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age",LongType)
        )
      )
    }

    /**
     * 缓冲去结构类型
     * @return
     */
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("sum",LongType),
          StructField("count",LongType)
        )
      )
    }

    /**
     * 函数计算结果数据类型 Out
     * @return
     */
    override def dataType: DataType = LongType

    /**
     * 函数稳定性
     * @return
     */
    override def deterministic: Boolean = true

    /**
     * 函数初始化
     * @param buffer
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit =  {
      buffer.update(0,0L);
      buffer.update(1,0L);
    }

    /**
     * 更新
     * @param buffer
     * @param input
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0) + input.getLong(0))
      buffer.update(1,buffer.getLong(1) + 1)
    }

    /**
     * 缓冲区合并
     * @param buffer1
     * @param buffer2
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))
    }

    /**
     * 计算聚合结果
     * @param buffer
     * @return
     */
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
