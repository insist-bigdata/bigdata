package top.damoncai.spark.sql.chapter03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.AggregatingAccumulator
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

object Spark_01_udaf2 {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("App")
    // 创建会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转化

    // 注册UDF
    spark.udf.register("avgAge", functions.udaf(new CustomAvgAge()))

    // DataFrame
    val df: DataFrame = spark.read.json("bigdata_08_spark/datas/json/user.json")

    // DataFrame - SQL
    df.createTempView("user")
    spark.sql("select avgAge(age) from user").show()

    //关闭资源
    spark.stop()
  }

  case class Buff(var sum:Long, var count:Long)

  /**
   * Spark3.0 版本可以采用强类型的 Aggregator 方式代替 UserDefinedAggregateFunction
   */
  class CustomAvgAge() extends Aggregator[Long,Buff,Long]{

    /**
     * 初始化数据
      * @return
     */
    override def zero: Buff = {
      Buff(0L,0L)
    }

    /**
     * 计算
     * @param b
     * @param a
     * @return
     */
    override def reduce(buff: Buff, input: Long): Buff = {
      buff.sum += input
      buff.count += 1
      buff
    }

    /**
     * 合并
     * @param b1
     * @param b2
     * @return
     */
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    /**
     * 结果
     * @param reduction
     * @return
     */
    override def finish(buff: Buff): Long = {
      buff.sum / buff.count
    }

    /**
     * 缓冲编码
     * @return
     */
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    /**
     * 输出编码
     * @return
     */
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
