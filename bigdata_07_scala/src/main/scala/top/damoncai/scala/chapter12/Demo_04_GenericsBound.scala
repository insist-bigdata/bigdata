package top.damoncai.scala.chapter12

object Demo_03_Generics {
  def main(args: Array[String]): Unit = {
    // 泛型上下限定
    def say[A <: Child](a:A): Unit = {
      println(a.getClass.getName)
    }

    say[Child](new Child())
  }
}

class Parent{}
class Child extends Parent{}
class SubChild extends Child{}

// 定义带泛型的集合类型
class MyCollection[+E]{}