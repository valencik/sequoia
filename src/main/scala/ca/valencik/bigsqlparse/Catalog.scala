package ca.valencik.bigsqlparse

import scala.collection.mutable.HashMap

case class Catalog private (schemaMap: HashMap[String, HashMap[String, Seq[String]]],
                            tempViews: HashMap[String, Seq[String]]) {

  def nameColumnInTable(relation: String)(c: String): Option[String] = {
    println(s"(nameColumnInTable) from $relation, with column $c")

    relation.split('.') match {
      case Array("public", table)   => if (lookupTableName(table).isDefined) Some(s"public.$table.$c") else None
      case Array("cteAlias", table) => if (lookupTableName(table).isDefined) Some(s"cteAlias.$table.$c") else None
      case Array(db, table) => {
        val dbMap = schemaMap.get(db)
        val columns = if (lookupTableName(table).isDefined && dbMap.isDefined) dbMap.get.get(table).getOrElse(tempViews.get(table).get) else Seq.empty
        if (columns.exists(_ == c.toLowerCase())) Some(s"$db.$table.$c") else None
      }
      case _ => None
    }
  }

  def lookupColumnInRelation(col: Identifier[_], relation: ResolvableRelation): Option[String] = relation match {
      case rr: ResolvedRelation => nameColumnInTable(rr.value)(col.name.asInstanceOf[String].toLowerCase())
      case _ => None
    }

  def lookupTableName(tn: String): Option[QualifiedName] = {
    tn.toLowerCase.split('.') match {
      case Array(table) =>
        if (tempViews.keySet.contains(table)) Some(QualifiedName(s"cteAlias.$table"))
        else Some(QualifiedName(s"public.$table"))
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

  def addTempViewColumn(table: String)(column: String): Unit = {
    val cs = tempViews.getOrElse(table, Seq.empty) :+ column
    tempViews.update(table, cs)
  }

  def lookupAndMaybeModify(alias: Option[RawIdentifier], qn: QualifiedName): Option[QualifiedName] = {
    val maybeQn = lookupQualifiedName(qn)
    // MUTATION
    if (maybeQn.isDefined && alias.isDefined)
      addTempViewColumn(alias.get.name.toLowerCase())(maybeQn.get.name.toLowerCase())
    maybeQn
  }

}
object Catalog {
  def apply(schema: HashMap[String, HashMap[String, Seq[String]]]): Catalog = new Catalog(schema, HashMap.empty)

  def apply(): Catalog = new Catalog(HashMap.empty, HashMap.empty)
}
