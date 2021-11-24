package top.damoncai.scala.chapter11

object Demo_06_MatchTupleExtend {
  def main(args: Array[String]): Unit = {
    // 扩展
    var (x,y) = ("张三",18)
    println(s"name:$x,age:$y")

    var List(a,b,c,_*) = List(1,2,3,4,5,6,7)
    println(s"a:$a,b:$b,z:$c")

    var fir :: sec :: rest = List(1,2,3,4,5,6)
    println(s"fir:$fir,sec:$sec,rest:$rest")

    println("=================")
    // 原本遍历
    var list:List[(String,Int)] = List(("a",1),("b",1),("c",1),("a",3))
    for (ele <- list) println(ele._1 + " " + ele._2)

    println("=================")
    //对变量赋值
    for ((word,count) <- list) println(word + " " +count)

    println("=================")
    for ((word,_) <- list) println(word)

    println("=================")
    // 过滤
    for (("a",count) <- list) println(count)
  }
}
