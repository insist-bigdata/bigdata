package top.damoncai.scala.chapter07

object Demo_02_FunctionParam {

  def main(args: Array[String]): Unit = {
//    （1）可变参数
      def fn1(name:String,hobbys:String*):Unit = {
        println(name)
        println(hobbys)
      }

//    （2）如果参数列表中存在多个参数，那么可变参数一般放置在最后
//    （3）参数默认值，一般将有默认值的参数放置在参数列表的后面
      def fn2(name:String,age:Int = 12):Unit = {
        println(name)
        println(age)
      }
//    （4）带名参数

      def fn3(name:String = "damoncai",age:Int = 12):Unit = {
        println(name)
        println(age)
      }

    fn1("damocnai2","pp","basketball")
    fn2("damocnai2")
    fn3(age=18);

  }
}
