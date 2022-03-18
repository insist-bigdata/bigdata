package top.damoncai.rtdw.dim

import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import top.damoncai.rtdw.bean.ProvinceInfo
import top.damoncai.rtdw.utils.{MyKafkaUtil, OffsetManagerUtil}

object ProvinceInfoApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setMaster("provinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_province"
    val groupId = "province_info_group"

    // 1.从Redis中获取Kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null;
    // Redis中偏移量
    if(kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId);
    }else{
      // Redis 中没有保存偏移量 Kafka 默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转 换为 HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //写入到 Hbase 中
    //写入到 Hbase 中
    offsetDStream.foreachRDD{
      rdd=>{
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            //得到从 kafka 中读取的 jsonString
            val jsonString: String = record.value()
            //转换为 ProvinceInfo
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString,
              classOf[ProvinceInfo])
            provinceInfo
          }
        }
        //保存到 hbase
        import org.apache.phoenix.spark._
        provinceInfoRDD.saveToPhoenix(
          "gmall2020_province_info",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("ha01,ha02,ha03:2181")
        )
        //提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
