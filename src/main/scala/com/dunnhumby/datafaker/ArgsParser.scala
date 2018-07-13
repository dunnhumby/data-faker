package com.dunnhumby.datafaker

object ArgsParser {

  def parseArgs(list: List[String]): Map[String, String] = {
    list match {
      case Nil => Map[String, String]()
      case "--num-cards" :: x :: xs => parseArgs(xs) + ("numCards" -> x)
      case "--records-per-card" :: x :: xs => parseArgs(xs) + ("recordsPerCard" -> x)
      case _ :: xs =>
        parseArgs(xs)
    }
  }

  def validateArgs(args: Map[String, String]) : Map[String, String] = {
    var validArgs = args
    if (!args.contains("numCards") && !args.contains("recordsPerCard")) {
      throw new Exception("at least one record must be created")
    }

    if (!args.contains("numCards")) {
      validArgs = validArgs + ("numCards" -> "0")
    }

    if (!args.contains("recordsPerCard")) {
      validArgs = validArgs + ("recordsPerCard" -> "0")
    }

    if (args.contains("recordsPerCard") & !args.contains("numCards")) {
      throw new Exception("'--records-per-card' requires '--num-cards'")
    }
    validArgs
  }
}