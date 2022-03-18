package top.damoncai.spark.streaming.chapter01_wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark_01_wordcount {

  def main(args: Array[String]): Unit = {

    //1.初始化 Spark 配置信息
    val sparkConf = new
        SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.通过监控端口创建DStream,读取进来的数据是一行一行
    val wordStream: ReceiverInputDStream[String] = ssc.socketTextStream("ha01.prdigital.cn", 9999)
    //4.将没一行数据切分
    val wsStreams: DStream[(String, Int)] = wordStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    //5.打印
    wsStreams.print()

    //6.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination();
  }

}
