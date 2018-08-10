package com.dunnhumby.datafaker

object ArgsParser {

  def parseArgs(list: List[String]): Map[String, String] = {
    list match {
      case Nil => Map.empty[String, String]
      case "--database" :: x :: xs => parseArgs(xs) + ("database" -> x)
      case "--file" :: x :: xs => parseArgs(xs) + ("file" -> x)
      case _ :: xs =>
        parseArgs(xs)
    }
  }

  def validateArgs(args: Map[String, String]): Map[String, String] = {
    args
  }
}