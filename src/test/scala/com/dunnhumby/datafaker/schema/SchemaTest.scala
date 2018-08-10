
package com.dunnhumby.datafaker.schema

import com.dunnhumby.datafaker.schema.table.SchemaTable
import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnFixed
import org.scalatest.{MustMatchers, WordSpec}

class SchemaTest extends WordSpec with MustMatchers {

  import com.dunnhumby.datafaker.schema.SchemaProtocol._
  import net.jcazevedo.moultingyaml._

  val baseString =
    """tables:
      |- name: table1_test
      |  rows: 100
      |  columns:
      |  - name: table1_column1_test
      |    column_type: Fixed
      |    data_type: Int
      |    value: 1
      |- name: table2_test
      |  rows: 200
      |  columns:
      |  - name: table2_column1_test
      |    column_type: Fixed
      |    data_type: String
      |    value: testing
      |  partitions:
      |  - table2_column1_test
    """.stripMargin

  "Schema" must {
    "read a Schema with tables" in {
      val string = baseString
      string.parseYaml.convertTo[Schema] mustBe {
        Schema(List(
           SchemaTable("table1_test", 100, List(
             SchemaColumnFixed("table1_column1_test", 1)), None),
           SchemaTable("table2_test", 200, List(
             SchemaColumnFixed("table2_column1_test", "testing")), Some(List("table2_column1_test")))))
      }
    }
  }
}
