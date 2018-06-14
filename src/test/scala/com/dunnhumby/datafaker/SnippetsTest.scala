package com.dunnhumby.datafaker

import java.sql.Date
import java.util.{Calendar, UUID}

import com.holdenkarau.spark.testing.{Column, DataframeGenerator, SharedSparkContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.WordSpec
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers

object SnippetsTest{
  def genDateRange(range: Range): Seq[Date] ={

    val calendar = Calendar.getInstance
    calendar.set(Calendar.YEAR, 2018)

    range.map{ i =>
      calendar.set(Calendar.DAY_OF_YEAR, i)
      new Date(calendar.getTimeInMillis)
    }
  }
}

/*
class SnippetsTest extends WordSpec with SharedSparkSession with Checkers {
  import SnippetsTest._
  import spark.implicits._

  "Given a partitioned table I " should {
    "be able to delete data from a partition without writing other partitions " in {

//      |-- card_id: long (nullable = true)
//      |-- prod_id: integer (nullable = true)
//      |-- store_id: integer (nullable = true)
//      |-- item_qty: integer (nullable = true)
//      |-- item_discount_amt: decimal(38,18) (nullable = true)
//      |-- spend_amt: decimal(38,18) (nullable = true)
//      |-- net_spend_amt: decimal(38,18) (nullable = true)
//      |-- date_id: date (nullable = true)


      val schema = StructType(List(
        StructField("card_id", LongType, true),
        StructField("prod_id", IntegerType, true),
        StructField("store_id", IntegerType, true),
        StructField("item_qty", IntegerType, true),
        StructField("item_discount_amt", DecimalType(38,18), true),
        StructField("spend_amt", DecimalType(38,18), true),
        StructField("net_spend_amt", DecimalType(38,18), true),
        StructField("date_id", DateType, true)))

      val itemDiscountGenerator = new Column("item_discount_amt", Gen.choose(0d, 100d).map(BigDecimal.valueOf) )
      val spendGenerator = new Column("spend_amt", Gen.choose(0d, 100d).map(BigDecimal.valueOf) )
      val netSpendGenerator = new Column("net_spend_amt", Gen.choose(0d, 100d).map(BigDecimal.valueOf) )


      val dateGenerator = new Column("date_id", Gen.oneOf(genDateRange(1 to 10)) )

      val dataframeGen: Arbitrary[DataFrame] = DataframeGenerator.arbitraryDataFrameWithCustomFields(spark.sqlContext, schema)(itemDiscountGenerator, spendGenerator, netSpendGenerator, dateGenerator)
      val property =
        forAll(dataframeGen.arbitrary) { dataframe =>
          val c = dataframe
          val tableName = appID + "_pt_test"
          dataframe.write.mode("overwrite").partitionBy("date_id").saveAsTable(tableName)

          val cardsToDelete = dataframe.select("card_id", "date_id").sample(false, fraction = 0.1)
          println(cardsToDelete.show(100, false))


          val newDf = spark.table(tableName)
          println("\n\naline " + c)
          println(dataframe.show(100, false))

          newDf.inputFiles.foreach(println)
          newDf.schema === schema &&
            newDf.count() == 100

        }

      check(property, MinSuccessful(1), MinSize(100))

    }
  }
}


*/
