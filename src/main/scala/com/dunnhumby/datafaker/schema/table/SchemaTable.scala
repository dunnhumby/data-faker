
package com.dunnhumby.datafaker.schema.table

import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import com.dunnhumby.datafaker.schema.table.columns.SchemaColumn

case class SchemaTable(name: String, rows: Long, columns: List[SchemaColumn], partitions: Option[List[String]])

object SchemaTableProtocol extends YamlParserProtocol {

  import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnProtocol._

  implicit val formatSchemaTable = yamlFormat4(SchemaTable.apply)

}