package top.damoncai.rtdw.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import top.damoncai.rtdw.bean.{OrderInfo, ProvinceInfo, UserStatus}
import top.damoncai.rtdw.utils.{ESUtil, MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}

import java.text.SimpleDateFormat
import java.util.Date

object OrderInfoApp {

  def main(args: Array[String]): Unit = {

    //1.从 Kafka 中查询订单信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group"

    //从 Redis 中读取 Kafka 偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      //Redis 中有偏移量 根据 Redis 中保存的偏移量读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      // Redis 中没有保存偏移量 Kafka 默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转 换为 HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recordDstream.transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

    //对从 Kafka 中读取到的数据进行结构转换，由 Kafka 的 ConsumerRecord 转换为一个 OrderInfo 对象
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo =
          JSON.parseObject(jsonString,classOf[OrderInfo])
        //通过对创建时间 2020-07-13 01:38:16 进行拆分，赋值给日期和小时属性，方便后续 处理
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        //获取日期赋给日期属性
        orderInfo.create_date = createTimeArr(0)
        //获取小时赋给小时属性
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }

    //方案 1：对 DStream 中的数据进行处理，判断下单的用户是否为首单
    //缺点：每条订单数据都要执行一次 SQL，SQL 执行过于频繁
//    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] =
//    orderInfoDStream.map {
//      orderInfo => {
//        //通过 phoenix 工具到 hbase 中查询用户状态
//        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id ='${orderInfo.user_id}'"
//        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
//        if (userStatusList != null && userStatusList.size > 0) {
//          orderInfo.if_first_order = "0"
//        } else {
//          orderInfo.if_first_order = "1"
//        }
//        orderInfo
//      }
//    }
//    orderInfoWithFirstFlagDStream.print(1000)

    //方案 2：对 DStream 中的数据进行处理，判断下单的用户是否为首单
    //优化:以分区为单位，将一个分区的查询操作改为一条 SQL
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] =
    orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //因为迭代器迭代之后就获取不到数据了，所以将迭代器转换为集合进行操作
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区内的用户 ids
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //从 hbase 中查询整个分区的用户是否消费过，获取消费过的用户 ids
        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id in('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //得到已消费过的用户的 id 集合
        val cosumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        //对分区数据进行遍历
        for (orderInfo <- orderInfoList) {
          // 注意：orderInfo 中 中 user_id 是 是 Long 类型，一定别忘了进行转换
          if (cosumedUserIdList.contains(orderInfo.user_id.toString)) {
            //如已消费过的用户的 id 集合包含当前下订单的用户，说明不是首单
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }

    //===============4.同批次状态修正=================
    //因为要使用 groupByKey 对用户进行分组，所以先对 DStream 中的数据结构进行转换
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] =
    orderInfoWithFirstFlagDStream.map {
      orderInfo => {
        (orderInfo.user_id, orderInfo)
      }
    }
    //按照用户 id 对当前采集周期数据进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] =
      orderInfoWithKeyDStream.groupByKey()
    //对分组后的用户订单进行判断
    val orderInfoRealWithFirstFlagDStream: DStream[OrderInfo] =
      groupByKeyDStream.flatMap {
        case (userId, orderInfoItr) => {
          //如果同一批次有用户的订单数量大于 1 了
          if (orderInfoItr.size > 1) {
            //对用户订单按照时间进行排序
            val sortedList: List[OrderInfo] = orderInfoItr.toList.sortWith(
              (orderInfo1, orderInfo2) => {
                orderInfo1.create_time < orderInfo2.create_time
              }
            )
            //获取排序后集合的第一个元素
            val orderInfoFirst: OrderInfo = sortedList(0)
            //判断是否为首单
            if (orderInfoFirst.if_first_order == "1") {
              //将除了首单的其它订单设置为非首单
              for (i <- 1 to sortedList.size - 1) {
                val orderInfoNotFirst: OrderInfo = sortedList(i)
                orderInfoNotFirst.if_first_order = "0"
              }
            }
            sortedList
          } else {
            orderInfoItr.toList
          }
      }
    }

    //5.订单与 Hbase 中的维度表进行关联
//    orderInfoRealWithFirstFlagDStream.mapPartitions{
//      orderInfoItr => {
//        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
//        //获取本批次中所有订单省份的 ID
//        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
//        //根据省份 id 到 Hbase 省份表中获取省份信息
//        var sql: String = s"select id,name,area_code,iso_code from gmall0713_province_info where id in('${provinceIdList.mkString("','")}')"
//        //{"id":"1","name":"zs","area_code":"1000","iso_code":"CN-JX"}
//        val provinceJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
//        //将 provinceInfoList 转换为 Map 集合 [id->{"id":"1","name":"zs","area_code":"1000","iso_code":"CN-JX"}]
//        val provinceJsonMap: Map[Long, JSONObject] = provinceJsonList.map {
//          proJsonObj => {
//            (proJsonObj.getLongValue("ID"), proJsonObj)
//          }
//        }.toMap
//
//        for(orderInfo <- orderInfoList) {
//          val province_id: Long = orderInfo.province_id
//          val provinceObj: JSONObject = provinceJsonMap.getOrElse(province_id, null)
//          if (provinceObj != null) {
//            orderInfo.province_iso_code = provinceObj.getString("ISO_CODE")
//            orderInfo.province_name = provinceObj.getString("NAME")
//            orderInfo.province_area_code = provinceObj.getString("AREA_CODE")
//          }
//        }
//        orderInfoList.toIterator
//      }
//    }

    //5.1 关联省份方案 2 使用广播变量，在 Driver 端进行一次查询 分区越多效果越明显 前提： 省份数据量较小
    val orderInfoWithProvinceDStream: DStream[OrderInfo] =
      orderInfoRealWithFirstFlagDStream.transform {
        rdd => {
          //每一个采集周期，都会在 Driver 端 执行从 hbase 中查询身份信息
          var sql: String = "select id,name,area_code,iso_code from gmall0713_province_info"
          val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
          //封装广播变量
          val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
            jsonObj => {
              val provinceInfo = ProvinceInfo(
                jsonObj.getString("ID"),
                jsonObj.getString("NAME"),
                jsonObj.getString("AREA_CODE"),
                jsonObj.getString("ISO_CODE")
              )
              (provinceInfo.id, provinceInfo)
            }
          }.toMap
          val provinceInfoBC: Broadcast[Map[String, ProvinceInfo]] =
            ssc.sparkContext.broadcast(provinceInfoMap)
          val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map {
            orderInfo => {
              val provinceBCMap: Map[String, ProvinceInfo] = provinceInfoBC.value
              val provinceInfo: ProvinceInfo =
                provinceBCMap.getOrElse(orderInfo.province_id.toString, null)
              if (provinceInfo != null) {
                orderInfo.province_name = provinceInfo.name
                orderInfo.province_area_code = provinceInfo.area_code
                orderInfo.province_iso_code = provinceInfo.iso_code
              }
              orderInfo
            }
          }
          orderInfoWithProvinceRDD
        }
      }

    //5.2 关联用户
    val orderInfoWithUserDStream: DStream[OrderInfo] =
      orderInfoWithProvinceDStream.mapPartitions {
        orderInfoItr => {
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          val userIdList: List[Long] = orderInfoList.map(_.user_id)
          //根据用户 id 到 Phoenix 中查询用户
          var sql: String =
            s"select id,user_level,birthday,gender,age_group,gender_name from gmall0713_user_info where id in('${userIdList.mkString("','")}')"
          val userJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
          val userJsonMap: Map[Long, JSONObject] = userJsonList.map(userJsonObj =>
            (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
          for (orderInfo <- orderInfoList) {
            val userJsonObj: JSONObject = userJsonMap.getOrElse(orderInfo.user_id,
              null)
            if (userJsonObj != null) {
              orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
              orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
            }
          }
          orderInfoList.toIterator
        }
      }
    orderInfoWithUserDStream.print(1000)

    //导入类下成员
    import org.apache.phoenix.spark._
    orderInfoWithFirstFlagDStream.foreachRDD {
      rdd => {
        //从所有订单中，将首单的订单过滤出来
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
        //获取当前订单用户并更新到 Hbase，注意：saveToPhoenix 在更新的时候，要求 rdd 中的属性 和插入 hbase 表中的列数必须保持一致，所以转换一下
        val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS2020",
          Seq("USER_ID", "IF_CONSUMED"),
          new Configuration,
          Some("ha01,ha02,ha03:2181")
        )
        //--------------3.2 将订单信息写入到 ES 中-----------------
        rdd.foreachPartition {
          orderInfoItr =>{
            val orderInfoList: List[(String,OrderInfo)] =
              orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString,orderInfo))
            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            ESUtil.bulkInsert(orderInfoList, "gmall2020_order_info_" + dateStr)
            //3.2 将订单信息推回 kafka 进入下一层处理 主题： dwd_order_info
            for ((id,orderInfo) <- orderInfoList) {
              //fastjson 要把 scala 对象包括 caseclass 转 转 json 字符串 需要加入,new SerializeConfig(true)
              MyKafkaSink.send("dwd_order_info",
                JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }
          }
        }
        //保存偏移量到 Redis
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }
    orderInfoWithFirstFlagDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
