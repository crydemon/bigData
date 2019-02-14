package practise

trait Monoid[A] {
  def op(a1: A, a2: A): A

  def zero: A
}

object Monoid {

  val stringMonoid = new Monoid[String] {
    def op(a1: String, a2: String) = a1 + a2

    val zero = ""
  }

  def listMonoid[A] = new Monoid[List[A]] {
    def op(a1: List[A], a2: List[A]) = a1 ++ a2

    val zero = Nil
  }

  val intAddition: Monoid[Int] = new Monoid[Int] {
    def op(x: Int, y: Int) = x + y

    val zero = 0
  }

  val intMultiplication: Monoid[Int] = new Monoid[Int] {
    def op(x: Int, y: Int) = x * y
    val zero = 1
  }

  val booleanOr: Monoid[Boolean] = new Monoid[Boolean] {
    def op(x: Boolean, y: Boolean) = x || y

    val zero = false
  }

  val booleanAnd: Monoid[Boolean] = new Monoid[Boolean] {
    def op(x: Boolean, y: Boolean) = x && y

    val zero = true
  }

  def optionMonoid[A]: Monoid[Option[A]] = new Monoid[Option[A]] {
    def op(x: Option[A], y: Option[A]): Option[A] = x orElse y
    val zero = None
  }

  // endo在什么里
  def endoMonoid[A]: Monoid[A => A] = new Monoid[A => A] {
    def op(f: A => A, g: A => A): A => A = f compose g
    val zero: A => A = (a: A) => a
  }

}

object monoidTest extends App {

  val a1 = "343"
  val a2 = "343"
  Monoid.stringMonoid.op(a1, a2)
  assert(Monoid.stringMonoid.op(a1, a2) == a1 + a2)
  assert(Monoid.stringMonoid.zero == "")

  val words = List("fdk", "ddd", "iii")
  println(words.reverse.foldRight("bbb")(Monoid.stringMonoid.op))
  println(words.foldLeft("bbb")(Monoid.stringMonoid.op))
}