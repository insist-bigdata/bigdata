package top.damoncai.rtdw.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import top.damoncai.rtdw.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}

object BaseDBMaxwellApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBCanalApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "gmall2020_db_m"
    val groupId = "base_db_maxwell_group"
    //从 Redis 中读取偏移量
    var recoredDStream: InputDStream[ConsumerRecord[String, String]] = null
    val kafkaOffsetMap: Map[TopicPartition, Long] =
      OffsetManagerUtil.getOffset(topic,groupId)
    if(kafkaOffsetMap!=null && kafkaOffsetMap.size >0){
      recoredDStream =
        MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recoredDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //获取当前采集周期中处理的数据 对应的分区已经偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recoredDStream.transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }
    //将从 kafka 中读取到的 recore 数据进行封装为 json 对象
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取 value 部分的 json 字符串
        val jsonStr: String = record.value()
        //将 json 格式字符串转换为 json 对象
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }
    //从 json 对象中获取 table 和 data，发送到不同的 kafka 主题
    jsonObjDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            val opType: String = jsonObj.getString("type")
            val tableName: String = jsonObj.getString("table")
            val dataObj: JSONObject = jsonObj.getJSONObject("data")
            if (dataObj != null && !dataObj.isEmpty) {
              if (
                ("order_info".equals(tableName) && "insert".equals(opType))
                  || (tableName.equals("order_detail") && "insert".equals(opType))
                  || tableName.equals("base_province")
                  || tableName.equals("user_info")
                  || tableName.equals("sku_info")
                  || tableName.equals("base_trademark")
                  || tableName.equals("base_category3")
                  || tableName.equals("spu_info")
              ) {
                //获取更新的表名
                val tableName: String = jsonObj.getString("table")
                //拼接发送的主题
                var sendTopic = "ods_" + tableName
                //向 kafka 发送消息
                MyKafkaSink.send(sendTopic, dataObj.toString())
              }
            }
          }
            //修改 Redis 中 Kafka 的偏移量
            OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
