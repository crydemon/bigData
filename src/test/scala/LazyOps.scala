object LazyOps {

  def init(): String = {
    println("call init()")
    return "kk"
  }

  def main(args: Array[String]) {
    lazy val property = init();
    //没有使用lazy修饰
    val s = Some("fkdjf")
    println(s.getOrElse("new"))
    println(s.orElse(Some("kkkk")))

    println("after init()")
    println(property)
  }

}

class A {
  private def v = 1
}

abstract class B extends A {
  def v: Int // make it abstract , compile error! why ?
}
