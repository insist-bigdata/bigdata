package top.damoncai.scala.chapter03

import java.io.{File, PrintWriter}

import top.damoncai.scala.chapter02.Student

import scala.io.Source

/**
 * 标准输入
 */
object Demo_05_DataType {

  def main(args: Array[String]): Unit = {
    var a:Byte = 127 //ERROR
//    var b:Byte = 128 //ERROR

    var c:Long = 123456L
    var d:Double = 222.222D
    var e:Float = 333.333F
    println(a)
    println(c)
    println(d)
    println(e)

    // 字符类型
    var c1:Char = 'a'

    var c2:Char = '\n'

    println("abc" + c2 + "def")

    // 空类型
    def f1():Unit = {
      println("f1被调用")
    }
    var f1V = f1()
    println(f1V)

    // 空引用
    var stu1:Student = new Student("张三",12);
    stu1 = null

//    //Nothing
//    def f2():Nothing = {
//      println("f2被调用")
//      throw NullPointerException;
//    }
  }
}
