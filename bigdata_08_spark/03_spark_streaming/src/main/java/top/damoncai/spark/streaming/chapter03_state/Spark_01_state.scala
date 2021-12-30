package top.damoncai.spark.streaming.chapter03_state

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object Spark_01_state {

  def main(args: Array[String]): Unit = {

    //1.初始化 Spark 配置信息
    val sparkConf = new
        SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)

    val pairs: DStream[(String, Int)] = dStream.map((_, 1))

    /**
     * 根据key对数据的状态进行更新
     * 参数一：表示相同的key的value数据
     * 参数二：表示缓冲区相同的key和value数据
     */
    val state: DStream[(String, Int)] = pairs.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
      val computedCount = buff.getOrElse(0) + seq.sum
      Option(computedCount)
    })

    state.print()

    ssc.start()
    ssc.awaitTermination();
  }
}
