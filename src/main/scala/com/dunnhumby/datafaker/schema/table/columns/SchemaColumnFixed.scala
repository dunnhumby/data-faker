package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import net.jcazevedo.moultingyaml.{deserializationError, YamlFormat, YamlString, YamlValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

case class SchemaColumnFixed[T](override val name: String, value: T) extends SchemaColumn {
  override def column: Column = lit(value)
}

trait SchemaColumnFixedProtocol extends YamlParserProtocol {

  implicit object SchemaColumnFixedFormat extends YamlFormat[SchemaColumnFixed[_]] {

    override def read(yaml: YamlValue): SchemaColumnFixed[_] = {
      val fields = yaml.asYamlObject.fields
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val YamlString(dataType) = fields.getOrElse(YamlString("data_type"), deserializationError(s"data_type not set for $name"))
      val value = fields.getOrElse(YamlString("value"), deserializationError(s"value not set for $name"))

      dataType match {
        case SchemaColumnDataType.Int => SchemaColumnFixed(name, lit(value.convertTo[Int]))
        case SchemaColumnDataType.Long => SchemaColumnFixed(name, lit(value.convertTo[Long]))
        case SchemaColumnDataType.Float => SchemaColumnFixed(name, lit(value.convertTo[Float]))
        case SchemaColumnDataType.Double => SchemaColumnFixed(name, lit(value.convertTo[Double]))
        case SchemaColumnDataType.Date => SchemaColumnFixed(name, lit(value.convertTo[Date]))
        case SchemaColumnDataType.Timestamp => SchemaColumnFixed(name, lit(value.convertTo[Timestamp]))
        case SchemaColumnDataType.String => SchemaColumnFixed(name, lit(value.convertTo[String]))
        case SchemaColumnDataType.Boolean => SchemaColumnFixed(name, lit(value.convertTo[Boolean]))
        case _ => deserializationError(s"unsupported data_type: $dataType for ${SchemaColumnType.Fixed}")
      }

    }

    override def write(obj: SchemaColumnFixed[_]): YamlValue = ???

  }

}