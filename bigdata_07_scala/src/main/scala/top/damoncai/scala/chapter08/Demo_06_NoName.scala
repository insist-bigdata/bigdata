package top.damoncai.scala.chapter08

object Demo_06_NoName {
  var p = new NPar {
    override val name: String = "张三"

    override def fun(): Unit = println("fun")
  }
}

abstract class NPar {
  val name:String;

  def fun():Unit;
}
