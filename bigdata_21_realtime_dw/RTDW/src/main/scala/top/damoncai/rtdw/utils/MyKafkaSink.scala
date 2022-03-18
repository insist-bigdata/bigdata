package top.damoncai.rtdw.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object MyKafkaSink {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")

  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null

  def createKafkaProducer:KafkaProducer[String,String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", broker_list)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idempotence",(true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  /**
   * 发送消息
   * @param topic
   * @param key
   * @param msg
   */
  def send(topic: String,key: String,msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }
}
