package top.damoncai.scala.chapter08

object Demo_11_TypeConvert {
  def main(args: Array[String]): Unit = {

    val eagleFish = new EagleFish()

    println(eagleFish.isInstanceOf[Fish])

    // 转换
    val fish:Fish = new EagleFish()
    val ef:EagleFish = fish.asInstanceOf[EagleFish]

  }
}
class Fish {
  def say(): Unit = {
    println("fish")
  }
}

class EagleFish extends Fish{
  override def say(): Unit = {
    println("fish")
  }
}
