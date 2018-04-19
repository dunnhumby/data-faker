package com.dunnhumby.datafaker

import com.holdenkarau.spark.testing.{LocalSparkContext, SparkContextProvider}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{EvilSparkContext, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll with SparkSessionProvider {
  self: Suite =>

  val spark: SparkSession = createSparkSession


  override def beforeAll() {

    super.beforeAll()
  }

  override def afterAll() {
    try {
      LocalSparkContext.stop(spark.sparkContext)
    } finally {
      super.afterAll()
    }
  }
}
