package ca.valencik.bigsqlparse

import scala.collection.immutable.HashMap

case class Catalog private (schemaMap: HashMap[String, HashMap[String, Seq[String]]],
                            tempViews: HashMap[String, Seq[String]]) {

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

  def lookupTableName(tn: String): Option[QualifiedName] = {
    tn.toLowerCase.split('.') match {
      case Array(table) =>
        if (tempViews.keySet.contains(table)) Some(QualifiedName(table)) else Some(QualifiedName(s"public.$table"))
      case Array(db, table) =>
        schemaMap.get(db).flatMap { d =>
          d.get(table).map { _ =>
            QualifiedName(s"$db.$table")
          }
        }
      case Array(schema, db, table) => Some(QualifiedName(s"$schema.$db.$table"))
      case _                        => None
    }
  }

  def lookupQualifiedName(qn: QualifiedName): Option[QualifiedName] = lookupTableName(qn.name)

  def addTempViewColumn(table: String)(column: String): Catalog = {
    val cs = tempViews.getOrElse(table, Seq.empty) :+ column
    this.copy(tempViews = tempViews.updated(table, cs))
  }

}
object Catalog {
  def apply(schema: HashMap[String, HashMap[String, Seq[String]]]): Catalog = new Catalog(schema, HashMap.empty)

  def apply(): Catalog = new Catalog(HashMap.empty, HashMap.empty)
}
