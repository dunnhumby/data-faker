package com.dunnhumby.datafaker.schema

import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import com.dunnhumby.datafaker.schema.table.SchemaTable

case class Schema(tables: List[SchemaTable])

object SchemaProtocol extends YamlParserProtocol {

  import com.dunnhumby.datafaker.schema.table.SchemaTableProtocol._

  implicit val formatSchema = yamlFormat1(Schema.apply)

}
