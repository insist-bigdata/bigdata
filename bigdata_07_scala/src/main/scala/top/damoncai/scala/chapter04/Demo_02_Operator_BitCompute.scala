package top.damoncai.scala.chapter04

object Demo_02_Operator_BitCompute {

  def main(args: Array[String]): Unit = {
    val st1:String = "hello"

    val st2:String = new String("hello");

    println(st1 == st2)
    println(st1.equals(st2))
    println(st1.eq(st2)) // scala中表示比较内存地址
  }
}
