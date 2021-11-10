package top.damoncai.scala.chapter03

import scala.io.StdIn

/**
 * 标准输入
 */
object Demo_03_StdIn {

  def main(args: Array[String]): Unit = {
    print("请输入姓名：")
    var name = StdIn.readLine()
    print("请输入年龄：")
    var age = StdIn.readInt()

    println(s"姓名：${age},年龄：${age}")
  }
}
