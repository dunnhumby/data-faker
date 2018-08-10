
package com.dunnhumby.datafaker.schema.table

import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnFixed
import org.scalatest.{MustMatchers, WordSpec}

class SchemaTableTest extends WordSpec with MustMatchers {

  import com.dunnhumby.datafaker.schema.table.SchemaTableProtocol._
  import net.jcazevedo.moultingyaml._

  val baseString =
    """name: test
      |rows: 10
      |columns:
      |- name: test_column
      |  column_type: Fixed
      |  data_type: Int
      |  value: 1
    """.stripMargin

  "SchemaTable" must {
    "read a Table with columns" in {
      val string = baseString
      string.parseYaml.convertTo[SchemaTable] mustBe SchemaTable("test", 10, List(SchemaColumnFixed("test_column", 1)), None)
    }

    "read a Table with columns and partitions" in {
      val string =
        s"""$baseString
           |partitions:
           |- test_column
         """.stripMargin
      string.parseYaml.convertTo[SchemaTable] mustBe SchemaTable("test", 10, List(SchemaColumnFixed("test_column", 1)), Some(List("test_column")))
    }

    "read a Table with columns and no partitions" in {
      val string =
        s"""$baseString
           |partitions:
         """.stripMargin
      string.parseYaml.convertTo[SchemaTable] mustBe SchemaTable("test", 10, List(SchemaColumnFixed("test_column", 1)), None)
    }
  }
}
