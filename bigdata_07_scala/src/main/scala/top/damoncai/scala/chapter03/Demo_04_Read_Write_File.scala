package top.damoncai.scala.chapter03

import java.io.{File, PrintWriter}

import scala.io.{Source, StdIn}

/**
 * 标准输入
 */
object Demo_04_Read_Write_File {

  def main(args: Array[String]): Unit = {
    // 读文件
    Source.fromFile("D:\\西门小狼狗\\4.MyGitHub\\bigdata\\bigdata_07_scala\\src\\main\\resources\\a.txt").foreach(print);

    //写文件
    val stream =  new PrintWriter(new File("D:\\西门小狼狗\\4.MyGitHub\\bigdata\\bigdata_07_scala\\src\\main\\resources\\output.txt"))
    stream.write("hello world")
    stream.close();
  }
}
