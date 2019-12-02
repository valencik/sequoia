package ca.valencik.sequoia

import cats.data.{Chain, EitherT, ReaderWriterState}

sealed trait RawName {
  def value: String
}
final case class RawTableName(value: String)  extends RawName
final case class RawColumnName(value: String) extends RawName

sealed trait ResolvedName {
  def value: String
}
final case class ResolvedTableName(value: String)  extends ResolvedName
final case class ResolvedTableAlias(value: String) extends ResolvedName
final case class ResolvedColumnName(value: String) extends ResolvedName

case class Catalog(c: Map[String, List[String]]) {
  def relationIsInCatalog(rt: RawName): Boolean = c.contains(rt.value)
  def maybeGetRelation(r: RawName): Option[(String, List[String])] =
    c.get(r.value).map(cs => (r.value, cs))
}
case class Resolver private (
    // TODO perhaps Resolver has a stack of Resolvers?
    t: Map[String, List[String]], // temps and ctes in scope
    r: List[String],              // relations in scope
    s: List[String]               // projection in current scope's SELECT clause
) {
  def addRelationToScope(k: String, v: List[String]): Resolver =
    this.copy(t = t.updated(k, v), r = k :: r)
  def relationIsAlias(rt: RawName): Boolean  = t.contains(rt.value)
  def addAliasToScope(rt: RawName): Resolver = this.copy(r = rt.value :: r)

  def addColumnToProjection(rc: String): Resolver = this.copy(s = rc :: s)
  def columnIsInScope(rc: RawName): Boolean =
    r.exists(rr => t.get(rr).map(cols => cols.contains(rc.value)).getOrElse(false))

  def addCTE(alias: String): Resolver = this.copy(t = t.updated(alias, s))
  def resetRelationScope(): Resolver  = this.copy(r = List.empty, s = List.empty)
}
object Resolver {
  def apply(): Resolver = new Resolver(Map.empty, List.empty, List.empty)
}

object MonadSqlState extends App {
  type Log          = Chain[String]
  type RState[A]    = ReaderWriterState[Catalog, Log, Resolver, A]
  type EitherRes[X] = EitherT[RState, ResolutionError, X]

  case class ResolutionError(value: RawName)

  def logUnresolvedColumn[I](
      column: ColumnRef[I, RawName],
      state: Resolver,
      catalog: Catalog
  ): String = {
    val col = column.value.value
    val rs  = state.r.mkString(",")
    val c   = catalog.c.toString
    s"Column '${col}' was not resolvable with relations: '${rs}' in catalog '${c}'"
  }

  def resolveTableRef[I](
      tr: TableRef[I, RawName]
  ): RState[Either[ResolutionError, TableRef[I, ResolvedName]]] = ReaderWriterState { (cat, res) =>
    if (res.relationIsAlias(tr.value))
      (
        Chain(s"Table '${tr.value.value}' is an alias in scope"),
        res.addAliasToScope(tr.value),
        Right(TableRef(tr.info, ResolvedTableAlias(tr.value.value)))
      )
    else
      cat.maybeGetRelation(tr.value) match {
        case Some((tn, cols)) =>
          (
            Chain(s"Table '${tn}' was in catalog"),
            res.addRelationToScope(tn, cols),
            Right(TableRef(tr.info, ResolvedTableName(tn)))
          )
        case None =>
          (Chain(s"Unresolved table '${tr.value.value}'"), res, Left(ResolutionError(tr.value)))
      }
  }

  def resolveColumnRef[I](
      col: ColumnRef[I, RawName]
  ): RState[Either[ResolutionError, ColumnRef[I, ResolvedName]]] = ReaderWriterState { (cat, res) =>
    if (res.columnIsInScope(col.value))
      (
        Chain(s"Resolved Column '${col.value.value}'"),
        res.addColumnToProjection(col.value.value),
        Right(ColumnRef(col.info, ResolvedColumnName(col.value.value)))
      )
    else
      (Chain(logUnresolvedColumn(col, res, cat)), res, Left(ResolutionError(col.value)))
  }

}
