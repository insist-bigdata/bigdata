package top.damoncai.scala.chapter11

object Demo_08_MatchCaseClass_MatchObj {
  def main(args: Array[String]): Unit = {
    // 匹配对象
    var bobe = new Teacher("bobe",12);

    bobe match {
      case Teacher("bobe",12) => println("name:bobe,age:12")
      case _ => println("else")
    }
  }
}

case class Teacher(name:String,age:Int)

