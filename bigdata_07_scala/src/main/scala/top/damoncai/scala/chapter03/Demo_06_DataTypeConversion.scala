package top.damoncai.scala.chapter03

object Demo_06_DataTypeConversion {

  def main(args: Array[String]): Unit = {
    // (byte,short) 和 char类型不可以自动转化

    var b1:Byte = 1
    var s1:Short = 1
    var c1:Char = '1'

    //    b1 = c1 // error 编译不报错(IDEA问题) 运行报错

    b1 = c1.toByte
    println(b1)

  }

}
