
package com.dunnhumby.datafaker

import java.io.File
import com.holdenkarau.spark.testing.LocalSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}


trait SharedSparkSession {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Test")
      .getOrCreate()
  }

}
