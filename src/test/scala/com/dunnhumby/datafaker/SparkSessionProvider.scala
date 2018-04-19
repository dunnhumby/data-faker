package com.dunnhumby.datafaker

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def appID: String = (this.getClass.getSimpleName
    + math.floor(math.random * 10E4).toLong.toString)

  def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost")
  }

  def createSparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
  }


}
