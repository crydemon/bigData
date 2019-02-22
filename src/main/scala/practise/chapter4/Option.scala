package practise.chapter4

import practise.chapter4.Option.sequence

import scala.{Either => _, Option => _, Some => _, _}
// hide std library `Option`, `Some` and `Either`,
// since we are writing our own in this chapter

sealed trait Option[+A] {
  def map[B](f: A => B): Option[B] = this match {
    case None => None
    case Some(a) => Some(f(a))
  }

  def flatMap[B](f: A => Option[B]): Option[B] = this match {
    case None => None
    case Some(a) => f(a)
  }

  def flatMap1[B](f: A => Option[B]): Option[B] =
    map(f) getOrElse None

  //b为父类型， default 不是立即求值
  def getOrElse[B >: A](default: => B): B = this match {
    case None => default
    case Some(b) => b
  }

  def orElse[B >: A](ob: => Option[B]): Option[B] =
    this map (Some(_)) getOrElse ob

  def filter(f: A => Boolean): Option[A] = this match {
    case Some(a) if (f(a)) => Some(a)
    case _ => None
  }

  def filter1(f: A => Boolean): Option[A] =
    flatMap(a =>
      if (f(a)) Some(a) else None
    )

  def lift[A, B](f: A => B): Option[A] => Option[B] = _ map f

  //Try 是一个通用函数，用于将一个基于异常的api转换为一个面向Option的API
  def Try[A](a: => A): Option[A] =
    try Some(a)
    catch {
      case e: Exception => None
    }


}

case class Some[+A](get: A) extends Option[A]

case object None extends Option[Nothing]

object Option {
  def mean(xs: Seq[Double]): Option[Double] =
    if (xs.isEmpty) None
    else Some(xs.sum / xs.length)

  def variance(xs: Seq[Double]): Option[Double] =
    mean(xs) flatMap (m => mean(xs.map(x => math.pow(x - m, 2))))


  def map2[A, B, C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] =
    (a, b) match {
      case (Some(aa), Some(bb)) => Some(f(aa, bb))
      case _ => None
    }

  def map3[A, B, C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] =
    a flatMap (aa => b map (bb => f(aa, bb)))

  def map5[A, B, C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] =
    for {
      aa <- a
      bb <- b
    } yield f(aa, bb)

  def map4[A, B, C, D](a: Option[A], b: Option[B], c: Option[C])(f: (A, B, C) => D): Option[D] =
    a flatMap (aa => b flatMap (bb => c map (cc => f(aa, bb, cc))))

  def sequence[A](a: List[Option[A]]): Option[List[A]] =
    a.foldRight[Option[List[A]]](Some(Nil))((x, y) => map3(x, y)(_ :: _))

  def traverse[A, B](a: List[A])(f: A => Option[B]): Option[List[B]] =
    a.foldRight[Option[List[B]]](Some(Nil))((x, y) => map3(f(x), y)(_ :: _))
}

object TestOption extends App {

  import Option._

  println(map4(Some(1), Some(2), Some(3))(_ + _ + _))
  println(map2(Some(1), Some(2))(_ + _))
  println(map3(Some(1), Some(2))(_ + _))
  println(mean(Seq()))
  println(mean(Seq(3, 4, 34, 3, 4, 34, 23, 4, 32, 4, 324, 234)))
}
