
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

case class SchemaColumnFixed[T](override val name: String, value: T) extends SchemaColumn {
  override def column(rowID: Option[Column] = None): Column = lit(value)
}

object SchemaColumnFixedProtocol extends SchemaColumnFixedProtocol
trait SchemaColumnFixedProtocol extends YamlParserProtocol {

  import net.jcazevedo.moultingyaml._

  implicit object SchemaColumnFixedFormat extends YamlFormat[SchemaColumnFixed[_]] {

    override def read(yaml: YamlValue): SchemaColumnFixed[_] = {
      val fields = yaml.asYamlObject.fields
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val YamlString(dataType) = fields.getOrElse(YamlString("data_type"), deserializationError(s"data_type not set for $name"))
      val value = fields.getOrElse(YamlString("value"), deserializationError(s"value not set for $name"))

      dataType match {
        case SchemaColumnDataType.Int => SchemaColumnFixed(name, value.convertTo[Int])
        case SchemaColumnDataType.Long => SchemaColumnFixed(name, value.convertTo[Long])
        case SchemaColumnDataType.Float => SchemaColumnFixed(name, value.convertTo[Float])
        case SchemaColumnDataType.Double => SchemaColumnFixed(name, value.convertTo[Double])
        case SchemaColumnDataType.Date => SchemaColumnFixed(name, value.convertTo[Date])
        case SchemaColumnDataType.Timestamp => SchemaColumnFixed(name, value.convertTo[Timestamp])
        case SchemaColumnDataType.String => SchemaColumnFixed(name, value.convertTo[String])
        case SchemaColumnDataType.Boolean => SchemaColumnFixed(name, value.convertTo[Boolean])
        case _ => deserializationError(s"unsupported data_type: $dataType for ${SchemaColumnType.Fixed}")
      }

    }

    override def write(obj: SchemaColumnFixed[_]): YamlValue = ???

  }

}