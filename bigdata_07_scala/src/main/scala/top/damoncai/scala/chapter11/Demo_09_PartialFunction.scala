package top.damoncai.scala.chapter11

object Demo_09_PartialFunction {

  def main(args: Array[String]): Unit = {
    var list = List(("a",1),("b",2),("c",3))

    var list2 = list.map(kv => (kv._1,kv._2 * 2))

    println("list2 ==> " + list2)

    var list3 = list.map(kv => {
      kv match {
        case (word,cont) => (word,cont *2 )
      }
    })
    println("list3 ==> " + list3)

    var list4 = list.map({
        case (word,cont) => (word,cont *2 )
    })
    println("list4 ==> " + list4)
  }

  def AAbs:PartialFunction[Int,Int] = {
    case i if i > 0 => i
  }

  def BAbs:PartialFunction[Int,Int] = {
    case i if i == 0 => 0
  }

  def CAbs:PartialFunction[Int,Int] = {
    case i if i < 0 => -i
  }

  def abs(num:Int):Int = (AAbs orElse BAbs orElse CAbs)(num)

  println(abs(-4))
}
