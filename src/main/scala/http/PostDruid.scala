package http

import scalaj.http.Http

object PostDruid extends App {

  //This example shows an HTTP POST, such as to a form:
  val response = Http("http://httpbin.org/post")
    .postForm
    .param("param1", "a")
    .param("param2", "b")
    .asString
  println(response)
}
