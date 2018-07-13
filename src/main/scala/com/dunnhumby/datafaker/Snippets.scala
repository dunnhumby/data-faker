package com.dunnhumby.datafaker

import com.dunnhumby.datafaker.Application.spark
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.mortbay.component.Container.Relationship


object Snippets {

  case class TableDesc(name:String, columns: Array[String])

  case class TableNode(name: String, edges: scala.collection.mutable.Map[String, Seq[TableNode]])

  def apply(spark: SparkSession): Snippets ={
    new Snippets(spark)
  }
}

class Snippets(spark: SparkSession) extends Serializable {
  import Snippets._
  import spark.implicits._


  private[datafaker] def insertOverwriteDynamic = {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
    spark.table("transaction_item_fct").where("card_id = 10").write.mode("overwrite").partitionBy("date_id").saveAsTable("transaction_item_fct_temp")

    spark.table("transaction_item_fct_temp").write.mode("overwrite").insertInto("transaction_item_fct")
    spark.catalog.refreshTable("transaction_item_fct")
  }

  private[datafaker] def insertOverwriteDynamicSql = {
    spark.table("transaction_item_fct").where("card_id = 10").write.mode("overwrite").partitionBy("date_id").saveAsTable("transaction_item_fct_temp")
    val ts = spark.sql(s"select * from transaction_item_fct_temp")
    ts.show(10)
    println(s"Count transaction_item_fct_temp" + ts.count())

    Thread.sleep(2000)
    // Write the data into disk as Hive partitions
    spark.sql(
      s"""
         |-- Load data into partitions dynamically
         |SET hive.exec.dynamic.partition = true;
         |SET hive.exec.dynamic.partition.mode = nonstrict;
         |INSERT OVERWRITE TABLE transaction_item_fct
         |PARTITION(dt)
         |SELECT card_id, prod_id, store_id, item_qty, item_discount_amt, spend_amt, net_spend_amt, date_id as dt
         |FROM transaction_item_fct_temp
    """.stripMargin).show()

    spark.catalog.refreshTable("transaction_item_fct")
  }

  private[datafaker] def deletePartitonsByDate(): Unit = {
    val inputs = spark.sql("select * from transaction_item_fct").where("date_id < '2018-03-21'").inputFiles

    inputs.foreach(println)

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    val paths = inputs.map(f => new Path(f))

    paths.foreach(hdfs.delete(_, true))

    spark.sessionState.catalog.getTableMetadata(TableIdentifier("transaction_item_fct"))

    spark.catalog.refreshTable("transaction_item_fct")
  }

  def populateTables(): List[TableDesc] ={
    val tables = spark.catalog.listTables().collect()

    val tablesWithCols = tables.map{t =>
      println(s"Table $t")
      val cols = spark.catalog.listColumns(t.database, t.name).map(f => f.name).collect()
      TableDesc(t.name, cols)
    }
    tablesWithCols.toList
  }

//  def buildGraph(desc: List[TableDesc], colMap: Map[String, TableDesc]): TableNode ={
//
//    def recurse(cur: TableNode, desc: List[TableDesc]): Unit ={
//      desc match {
//        case Nil => cur
//        case t => TableNode()
//      }
//    }
//
//  null
//  }

  def processNodes(tables: List[TableDesc]): Map[(TableDesc, String), TableDesc] ={

    findEdge(tables.head, tables.tail, tables)
  }

  def findEdge(table: TableDesc, left: List[TableDesc], tables: List[TableDesc]): Map[(TableDesc, String), TableDesc] = left match {
    case Nil => findLeafNodes(table, tables)
    case x::xs =>
      findLeafNodes(table, tables) ++ findEdge(x, xs, tables)

  }

  def findLeafNodes(table: TableDesc, tables: List[TableDesc]): Map[(TableDesc, String), TableDesc] = {
    val removedCurrent = tables.filterNot{ f =>
      table.name == f.name
    }
    table.columns.flatMap(locateCol(_, removedCurrent)).toMap
      .map(f => (table, f._1) -> f._2)
  }

  def locateCol(col: String, tables: List[TableDesc]): Map[String, TableDesc] ={
    (for{
      t <- tables;
      c <- t.columns if c == col
    } yield (c -> t)) toMap
  }

  def columnsToTables(tds: Seq[TableDesc]): Map[String, TableDesc] ={
    tds.map(f => (f.columns, f)).map{ f =>
      f._1.map(b => (b, f._2))
    }.flatten.toMap
  }
}
