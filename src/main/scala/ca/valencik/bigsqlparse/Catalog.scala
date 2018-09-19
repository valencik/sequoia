package ca.valencik.bigsqlparse

import scala.collection.immutable.HashMap

class Catalog private (schemaMap: HashMap[String, HashMap[String, Seq[String]]]) {

  def nameColumnInTable(relation: String)(c: String): Option[String] = {
    val nameParts = relation.split('.').toList
    println(s"nameParts: $nameParts, from $relation, with column $c")
    if (nameParts.size == 1) {
      schemaMap.flatMap {
        case (schemaName, tables) =>
          tables.get(relation).flatMap { cs =>
            cs.filter { _.toLowerCase == c.toLowerCase }.map { case col => s"$schemaName.$relation.$col" }.headOption
          }
      }.headOption
    } else {
      val db    = nameParts.head
      val table = nameParts.tail.head
      println(s"db: $db, table: $table")
      val x = schemaMap.get(db).flatMap { tableMap =>
        tableMap.get(table).flatMap { cs =>
          cs.filter { _.toLowerCase == c.toLowerCase }.map { case col => s"$db.$table.$col" }.headOption
        }
      }
      println(s"x: $x")
      x
    }
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
object Catalog {
  def apply(schema: HashMap[String, HashMap[String, Seq[String]]]): Catalog = new Catalog(schema)

  def apply(): Catalog = new Catalog(HashMap.empty)
}
