package top.damoncai.scala.chapter12

object Demo_02_Implicit {

  def main(args: Array[String]): Unit = {

    // 隐式函数（定义在前面）
    implicit def convert(num:Int):MyInt = new MyInt(num)

    var num:Int = 12

    // 当想调用对象功能时，如果编译错误，那么编译器会尝试在当前作用域范
    // 围内查找能调用对应功能的转换规则，这个调用过程是由编译器完成的，所以称之为隐
    // 式转换。也称之为自动转换
    println(num.maxBy(13))

    //隐式类
    implicit class MyInt2(var num:Int){
      def maxBy2(other:Int):Int = if(num > other) num else other
      def minBy2(other:Int):Int = if(num < other) num else other
    }
    println(num.maxBy2(8))

    // 隐式参数
    def say(name:String = "张三"):Unit = {
      println("hello," + name)
    }

    say()
    say("李四")

    implicit var name:String = "小白"
    def say2(implicit name:String):Unit = {
      println("hello," + name)
    }
    say2
  }
}

class MyInt(var num:Int) {
  def maxBy(other:Int):Int = if(num > other) num else other
  def minBy(other:Int):Int = if(num < other) num else other
}
