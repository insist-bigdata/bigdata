package top.damoncai.scala.chapter08

// 伴生对象
object Demo_07_AssociatedObj {

  def main(args: Array[String]): Unit = {
    // 伴生对象中的属性和方法都可以通过伴生对象名（类名）直接调用访问
    val name = Cat.oName
    println(name)
  }
}

class Cat {
  val cName = "张三"
}

object Cat {
  val oName = "李四"
}


