package top.damoncai.scala.chapter06

object Demo_01_if {


  def main(args: Array[String]): Unit = {


    println("=======================  if返回值  =======================")
    var age = 10;

    // any也可以省略
    var res1:Any = if(age > 18) {
      "成年人"
    }else if(age == 18) {
      "刚成年"
    }else{
      17
    }
    println(res1)

    println("=======================  if三元运算  =======================")

    var res2 = if( age > 18) "成年人" else "17"
    println(res2)
  }
}
