package practise

import practise.Props.{FailedCase, SuccessCount}

sealed trait Result {
  def isFalsified:Boolean
}
case object Passed extends Result{
  override def isFalsified: Boolean = false
}
case class Falsified(failure:FailedCase, success:SuccessCount) extends Result{
  override def isFalsified: Boolean = true
}
