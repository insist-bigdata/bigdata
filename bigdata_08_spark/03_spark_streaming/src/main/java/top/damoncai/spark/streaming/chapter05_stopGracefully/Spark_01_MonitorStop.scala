package top.damoncai.spark.streaming.chapter05_stopGracefully

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.net.URI
import scala.util.Random

class Spark_01_MonitorStop(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {
    val fs: FileSystem = FileSystem.get(new URI("hdfs://linux1:9000"), new
        Configuration(), "atguigu")
    while (true) {
      try
        Thread.sleep(5000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      val state: StreamingContextState = ssc.getState
      val bool: Boolean = fs.exists(new Path("hdfs://linux1:9000/stopSpark"))
      if (bool) {
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
