package com.dunnhumby.datafaker.schema.table.columns

import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import net.jcazevedo.moultingyaml.{deserializationError, YamlFormat, YamlString, YamlValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

case class SchemaColumnExpression(override val name: String, expression: String) extends SchemaColumn {
  override def column: Column = expr(expression)
}

trait SchemaColumnExpressionProtocol extends YamlParserProtocol {

  implicit object SchemaColumnExpressionFormat extends YamlFormat[SchemaColumnExpression] {

    override def read(yaml: YamlValue): SchemaColumnExpression = {
      val fields = yaml.asYamlObject.fields
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val YamlString(expression) = fields.getOrElse(YamlString("expression"), deserializationError(s"value not set for $name"))
      SchemaColumnExpression(name, expression)
    }

    override def write(obj: SchemaColumnExpression): YamlValue = ???

  }

}