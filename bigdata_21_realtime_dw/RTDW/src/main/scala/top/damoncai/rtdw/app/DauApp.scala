package top.damoncai.rtdw.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import top.damoncai.rtdw.bean.DauInfo
import top.damoncai.rtdw.utils.{ESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

import java.text.SimpleDateFormat
import java.util.Date
import java.lang
import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "gmall_dau_bak"
    val topic = "gmall_start_0523"

    //从 Redis 中读取 Kafka 偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null

    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      //Redis 中有偏移量 根据 Redis 中保存的偏移量读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,kafkaOffsetMap,groupId)
    }else{
      // Redis 中没有保存偏移量 Kafka 默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }

    // 得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转换为 HasOffsetRanges，会自动记录位置

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
      }
    }

    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map { record =>
      //获取启动日志
      val jsonStr: String = record.value()
      //将启动日志转换为 json 对象
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      //获取时间戳 毫秒数
      val ts: lang.Long = jsonObj.getLong("ts")
      //获取字符串 日期 小时
      val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      //对字符串日期和小时进行分割，分割后放到 json 对象中，方便后续处理
      val dateHour: Array[String] = dateHourString.split(" ")
      jsonObj.put("dt",dateHour(0))
      jsonObj.put("hr",dateHour(1))
      jsonObj
    }

    jsonObjDStream.print()

    // Redis去重 - 方法一 存在缺陷
//    val filterJsonObjDStream: DStream[JSONObject] = jsonObjDStream.filter{
//      jsonObj => {
//        val jedisClient: Jedis = MyRedisUtil.getJedisClient
//        //当前日期
//        val dt = jsonObj.getString("dt")
//        // 设备
//        val mid: String = jsonObj.getJSONObject("common").getString("mid")
//
//        val key: String = "dau:" + dt
//        // 判断Redis中是否存在数据
//        val isNew: lang.Long = jedisClient.sadd(key, mid)
//        if (isNew == 1L) { // 第一次添加
//          jedisClient.expireAt(key,3600*24) // 设置过期时间
//          jedisClient.close()
//          true
//        } else { // 已存在
//          jedisClient.close()
//          false
//        }
//      }
//    }

    //方案 2 以分区为单位进行过滤，可以减少和连接池交互的次数
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //获取 Redis 客户端
        val jedisClient: Jedis = MyRedisUtil.getJedisClient()
        //定义当前分区过滤后的数据
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        for (jsonObj <- jsonObjItr) {
          //获取当前日期
          val dt: String = jsonObj.getString("dt")
          //获取设备 mid
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接向 Redis 放的数据的 key
          val dauKey: String = "dau:" + dt
          //判断 Redis 中是否存在该数据
          val isNew: lang.Long = jedisClient.sadd(dauKey,mid)
          //设置当天的 key 数据失效时间为 24 小时
          jedisClient.expire(dauKey,3600*24)
          if (isNew == 1L) {
            //如果 Redis 中不存在，那么将数据添加到新建的 ListBuffer 集合中，实现过滤的效果
            filteredList.append(jsonObj)
          }
        }
        jedisClient.close()
        filteredList.toIterator
      }
    }
    //输出测试 数量会越来越少，最后变为 0 因为我们 mid 只是模拟了 50 个

    // 向ES中插入数据
    filteredDStream.foreachRDD{
      rdd=>{//获取 DS 中的 RDD
        rdd.foreachPartition{//以分区为单位对 RDD 中的数据进行处理，方便批量插入
          jsonItr=>{
            val dauList: List[(String,DauInfo)] = jsonItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                jsonObj.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList
            //对分区的数据进行批量处理
            //获取当前日志字符串
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            ESUtil.bulkInsert(dauList,"gmall2020_dau_info_" + dt)
          }
        }
        //在保存最后提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

//    jsonObjDStream.foreachRDD(jsonObjRDD => {
//    })
    //测试输出 2
    ssc.start()
    ssc.awaitTermination()
  }
}
