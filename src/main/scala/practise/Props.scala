package practise

import org.scalacheck.Gen
import practise.Props.{FailedCase, SuccessCount}

trait Props {
  def check: Either[(FailedCase, SuccessCount), SuccessCount]

  def listOfN[A](n: Int, a: Gen[A]): Gen[List[A]]

  def forAll[A](a: Gen[A])(f: A => Boolean): Props

  def &&(p: Props): Props
}


object Props {
  type FailedCase = String
  type SuccessCount = Int
  type TestCases = Int
  type Result = Either[(FailedCase, SuccessCount), SuccessCount]

  case class Props(run: TestCases => Result)

}
