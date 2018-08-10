
package com.dunnhumby.datafaker.schema.table.columns

import org.scalatest.{MustMatchers, WordSpec}

class SchemaColumnExpressionTest extends WordSpec with MustMatchers {

  import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnExpressionProtocol._
  import net.jcazevedo.moultingyaml._

  val name = "test"
  val column_type = "Expression"

  val baseString =
    s"""name: $name
       |column_type: $column_type
    """.stripMargin

  "SchemaColumnExpression" must {
    "read an Expression column" in {
      val string =
        s"""$baseString
           |expression: concat('testing_', round(rand() * 10, 2))
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnExpression] mustBe SchemaColumnExpression(name, "concat('testing_', round(rand() * 10, 2))")
    }
  }
}
