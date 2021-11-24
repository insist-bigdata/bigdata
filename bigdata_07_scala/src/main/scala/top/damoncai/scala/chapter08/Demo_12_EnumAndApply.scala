package top.damoncai.scala.chapter08

object Demo_12_EnumAndApply {
  def main(args: Array[String]): Unit = {
    println(CustomWeek.MON)
    println(CustomWeek.MON.id)
  }
}

object CustomWeek extends Enumeration {
  val MON = Value(1,"MONN")
}

object TestApp extends App {
  println("TestApp ~~")
}
