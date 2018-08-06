package ca.valencik.bigsqlparse

import scala.collection.immutable.HashMap

class Catalog(schemaMap: HashMap[String, HashMap[String, Seq[String]]]) {
  type Column     = String
  type TableName  = String
  type SchemaName = String
  type Schema     = HashMap[SchemaName, HashMap[TableName, Column]]

  def nameColumn(c: String): Option[String] = {
    schemaMap.flatMap {
      case (schemaName, tables) =>
        tables.flatMap {
          case (tableName, columns) =>
            columns
              .filter { _.toLowerCase == c.toLowerCase }
              .map { case col => s"$schemaName.$tableName.$col" }
        }
    }.headOption
  }

  def nameTable(t: String): Option[String] = {
    schemaMap.flatMap {
      case (schemaName, tables) =>
        tables
          .filter { case (tableName, _) => tableName.toLowerCase == t.toLowerCase }
          .map { case (table, _) => s"$schemaName.$table" }
    }
  }.headOption

}
