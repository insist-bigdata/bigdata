package top.damoncai.spark.streaming.chapter02_receiver

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object Spark_01_receiver_diy {

  def main(args: Array[String]): Unit = {

    //1.初始化 Spark 配置信息
    val sparkConf = new
        SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val streams: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver())

    streams.print()

    ssc.start()
    ssc.awaitTermination();
  }

  /**
   * 集成 Receiver 自定义 泛型、和存储
   */
  class CustomerReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var state:Boolean = true

    override def onStart(): Unit = {
      new Thread {
        () -> {
          while (state) {
            val msg: String = new Random().nextInt().toString
            store(msg)
            Thread.sleep(1000)
          }
        }
      }.start()
    }

    override def onStop(): Unit = {
      state = false
    }
  }
}
