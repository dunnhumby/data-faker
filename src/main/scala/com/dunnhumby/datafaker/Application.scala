package com.dunnhumby.datafaker

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {

  val parsedArgs = ArgsParser.parseArgs(args.toList)
  ArgsParser.validateArgs(parsedArgs)

  val logger = Logger(getClass)
  val config1 = ConfigFactory.load
  val conf = new SparkConf()
    .set("hive.exec.dynamic.partition", "true")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .setAppName("data-faker")
  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val dg = DataGenerator(spark, spark.catalog.currentDatabase)
  dg.generateAndWriteCardDimCData(parsedArgs("numCards").toInt)
  dg.generateAndWriteTransactionItemFctData(parsedArgs("numCards").toInt, parsedArgs("recordsPerCard").toInt, "date_id")
  dg.generateAndWriteSuppressions(Array(("card_code", "card_code", 0.1f), ("hshd_code", "hshd_code", 0.2f)), Table.cards)

  spark.close()
}