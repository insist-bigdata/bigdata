package top.damoncai.rtdw.utils

import java.io.InputStreamReader
import java.util.Properties

object MyPropertiesUtil {

  /**
   * 测试
   */
  def main(args: Array[String]): Unit = {
    val properties: Properties = load("config.properties")
    println(properties)
  }

  def load(propertieName: String): Properties = {
    val prop = new Properties()
    prop.load(
      new InputStreamReader(
        Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) ,
        "UTF-8"
      )
    )
    prop
  }
}
