package com.dunnhumby.datafaker.schema.table.columns

import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.Column

abstract class SchemaColumn {
  def name: String

  def column: Column
}

object SchemaColumnDataType {
  val Int = "Int"
  val Long = "Long"
  val Float = "Float"
  val Double = "Double"
  val Date = "Date"
  val Timestamp = "Timestamp"
  val String = "String"
  val Boolean = "Boolean"
}

object SchemaColumnType {
  val Fixed = "Fixed"
  val Random = "Random"
  val Selection = "Selection"
  val Sequential = "Sequential"
  val Expression = "Expression"
}

object SchemaColumnProtocol extends YamlParserProtocol
  with SchemaColumnFixedProtocol
  with SchemaColumnRandomProtocol
  with SchemaColumnSelectionProtocol
  with SchemaColumnSequentialProtocol
  with SchemaColumnExpressionProtocol {

  implicit object SchemaColumnFormat extends YamlFormat[SchemaColumn] {

    override def read(yaml: YamlValue): SchemaColumn = {
      val fields = yaml.asYamlObject.fields
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val YamlString(columnType) = fields.getOrElse(YamlString("column_type"), deserializationError(s"column_type not set for $name"))

      columnType match {
        case SchemaColumnType.Fixed => yaml.convertTo[SchemaColumnFixed[_]]
        case SchemaColumnType.Random => yaml.convertTo[SchemaColumnRandom[_]]
        case SchemaColumnType.Selection => yaml.convertTo[SchemaColumnSelection[_]]
        case SchemaColumnType.Sequential => yaml.convertTo[SchemaColumnSequential[_]]
        case SchemaColumnType.Expression => yaml.convertTo[SchemaColumnExpression]
        case _ => deserializationError(s"unsupported column_type: $columnType")
      }

    }

    override def write(obj: SchemaColumn): YamlValue = ???

  }

}