package top.damoncai.scala.chapter12

object Demo_03_Generics {
  def main(args: Array[String]): Unit = {
    // 协变 和 逆变
    var co:MyCollection[Parent] = new MyCollection[Child]();
  }
}

class Parent{}
class Child extends Parent{}
class SubChild extends Child{}

// 定义带泛型的集合类型
class MyCollection[+E]{}