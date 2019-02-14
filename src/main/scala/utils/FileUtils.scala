package utils

import java.io.File
/**
  * 20170309
  * 目录操作
  */

object FileUtils {

  def main(args: Array[String]) {
    val path: File = new File("C:/Users/wei/ScalaWorkspace/learn0305")
    for (d <- subdirs(path))
      println(d)
  }

  //遍历目录
  def subdirs(dir: File): Iterator[File] = {

    val children = dir.listFiles.filter(_.isDirectory())
    children.toIterator ++ children.toIterator.flatMap(subdirs _)

  }

  //删除目录和文件
  def dirDel(path: File) {
    if (!path.exists())
      return
    else if (path.isFile()) {
      path.delete()
      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }

    path.delete()
    println(path + ":  目录被删除")

  }




}

