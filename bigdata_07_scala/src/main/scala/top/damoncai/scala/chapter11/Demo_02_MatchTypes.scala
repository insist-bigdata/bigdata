package top.damoncai.scala.chapter11

object Demo_02_MatchTypes {
  def main(args: Array[String]): Unit = {
    // 1.常量匹配
    def customerMatch(arg:Any):String = {
      arg match {
        case 1 => "one"
        case true => "真"
        case "hello" => "world"
        case _ => ""
      }
    }
    println(customerMatch("hello"))

    // 2.类型匹配
    def describe(x: Any) = x match {
      case i: Int => "Int"
      case s: String => "String hello"
      case m: List[_] => "List"
      case c: Array[Int] => "Array[Int]"
      case someThing => "something else " + someThing
    }

    //泛型擦除
    println(describe(List(1, 2, 3, 4, 5)))
    //数组例外，可保留泛型
    println(describe(Array(1, 2, 3, 4, 5, 6)))
    println(describe(Array("abc")))
  }
}
