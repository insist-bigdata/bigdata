package top.damoncai.scala.chapter02


class Student (name:String,age:Int) {

  def printInfo(): Unit = {
    println(name + " - " + age + " - " + Student.school);
  }
}


// 引入伴生对象
object Student {
  val school:String = "mashibing";

  def main(args: Array[String]): Unit = {
   val stu = new Student("张三",12);
    stu.printInfo();
  }
}
