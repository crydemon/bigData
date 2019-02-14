package practise

import org.scalacheck.{Gen, Prop}


trait RNG {
  def nextInt: (Int, RNG)
}

case class SimpleRNG(seed: Long) extends RNG {
  override def nextInt: (Int, RNG) = {
    val newSeed = (seed * 0x5DEECE66DL + 0xBL)
    val nextRNG = SimpleRNG(newSeed)
    val n = (newSeed >>> 16).toInt
    (n, nextRNG)
  }
}

object test extends App {


  type Rand[+A] = RNG => (A, RNG)

  def unit[A](a: A): Rand[A] =
    rng => (a, rng)

  def map[A, B](s: Rand[A])(f: A => B): Rand[B] =
    rng => {
      val (a, rng2) = s(rng)
      (f(a), rng2)
    }

  def nonNegativeInt(rng: RNG): (Int, RNG) = {
    val result = rng.nextInt
    if (result._1 > 0)
      result
    else
      nonNegativeInt(result._2)
  }


  def nonNegativeEven: Rand[Int] =
    map(nonNegativeInt)(i => i - i % 2)

  val int: Rand[Int] = _.nextInt
  println(int(SimpleRNG(42)).getClass)
  println(SimpleRNG(-1).nextInt)
  println(SimpleRNG(42).nextInt)
  println(nonNegativeInt(SimpleRNG(-1)))
}


object ScalaCheck extends App {

  import org.scalacheck.Gen


  val intList = Gen.listOf(Gen.choose(0, 100))

  val prop = Prop.forAll(intList)(ns => ns.reverse.reverse == ns) &&
    Prop.forAll(intList)(ns => ns.headOption == ns.reverse.lastOption)
  val failingProp = Prop.forAll(intList)(ns => ns.reverse == ns)
  println(prop.check())

  println(failingProp.check())
}



















