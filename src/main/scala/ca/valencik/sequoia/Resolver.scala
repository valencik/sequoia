package ca.valencik.sequoia

import cats.data.State
import cats.implicits._

case class Resolver private (
    c: Map[String, Set[String]], // immutable global database catalog
    ctes: Map[String, Set[String]], // ctes in scope
    r: Set[String], // relations in scope
    s: Set[String] // projection in current scope's SELECT clause
) {
  def relationIsInCatalog(rt: RawName): Boolean = c.contains(rt.value)
  def addRelationToScope(rt: RawName): Resolver = this.copy(r = r + rt.value)
  def relationIsAlias(rt: RawName): Boolean     = ctes.contains(rt.value)
  def addAliasToScope(rt: RawName): Resolver    = this.copy(r = r + rt.value)

  // TODO Can I clean up these flattens?
  // TODO I should be careful not to lose dups by using Set, I want to report these as errors
  lazy val columnsInScope: Set[String] = r.flatMap { rn =>
    Set(c.get(rn), ctes.get(rn)).flatten
  }.flatten
  def columnIsInScope(rc: RawName): Boolean = columnsInScope(rc.value)

  def addCTE(alias: String, cols: Set[String]): Resolver = this.copy(ctes = ctes.updated(alias, cols))
  def resetRelationScope(): Resolver                     = this.copy(r = Set.empty, s = Set.empty)
}
object Resolver {
  def apply(c: Map[String, Set[String]]): Resolver = new Resolver(c, Map.empty, Set.empty, Set.empty)

  type RState[A] = State[Resolver, A]

  private def getSelectColsName[I](sc: SelectCols[ResolvedName, I]): RState[List[String]] =
    State.inspect { acc =>
      sc.cols.zipWithIndex.flatMap {
        case (selection, index) =>
          selection match {
            // TODO handle optional tableref
            case SelectStar(_, _) => acc.columnsInScope.toList
            case SelectExpr(_, e, a) =>
              List(
                a match {
                  case Some(ca) => ca.value
                  case None =>
                    e match {
                      case ColumnExpr(_, c)   => c.value.value
                      case ConstantExpr(_, _) => s"col_${index}"
                      // TODO not sure how Subqueries work for naming columns
                      case SubQueryExpr(_, _) => ???
                    }
                }
              )
          }
      }
    }

  private def getQueryColumnNames[I](q: Query[ResolvedName, I]): RState[List[String]] = q match {
    case QueryWith(_, _, q)   => getQueryColumnNames(q)
    case QuerySelect(_, qs)   => getSelectColsName(qs.select)
    case QueryLimit(_, _, qs) => getSelectColsName(qs.select)
  }

  def resolveOneRef(ref: RawName): RState[ResolvedName] = State { acc =>
    if (acc.relationIsInCatalog(ref))
      (acc.addRelationToScope(ref), ResolvedTableName(ref.value))
    else if (acc.relationIsAlias(ref))
      (acc.addAliasToScope(ref), ResolvedTableAlias(ref.value))
    else {
      (acc, UnresolvedTableName(ref.value))
    }
  }

  def resolveOneTablish[I](tablish: Tablish[RawName, I]): RState[Tablish[ResolvedName, I]] =
    State { acc =>
      {
        tablish match {
          // TODO support alias
          case TablishTable(i, a, r) => {
            // TODO I think this could be cleaned up with traverse instances for Tablish
            val (nacc, rref) = resolveOneRef(r.value).run(acc).value
            (nacc, TablishTable(i, a, TableRef(r.info, rref)))
          }
          case _ => ??? // TablishSubquery(i, a, resolveQuery(q))
        }
      }
    }

  def resolveFrom[I](from: From[RawName, I]): RState[From[ResolvedName, I]] = State {
    acc =>
      {
        // TODO perhaps clean with a for-yield? if not, a traverse
        val (nacc, rrels) = from.rels.traverse(resolveOneTablish).run(acc).value
        (nacc, From(from.info, rrels))
      }
  }

  def resolveOneCol(col: RawName): RState[ResolvedName] = State.inspect { acc =>
    if (acc.columnIsInScope(col))
      ResolvedColumnName(col.value)
    else
      UnresolvedColumnName(col.value)
  }

  def resolveExpression[I](exp: Expression[RawName, I]): RState[Expression[ResolvedName, I]] =
    State { acc =>
      exp match {
        case ColumnExpr(i, col) => {
          val (nacc, rcol) = resolveOneCol(col.value).run(acc).value
          (nacc, ColumnExpr(i, ColumnRef(col.info, rcol)))
        }
        case _ => ???
      }
    }

  def resolveSelection[I](selection: Selection[RawName, I]): RState[Selection[ResolvedName, I]] =
    State { acc =>
      selection match {
        case SelectExpr(i, e, a) => {
          val (nacc, re) = resolveExpression(e).run(acc).value
          (nacc, SelectExpr(i, re, a))
        }
        case _ => ???
      }
    }

  def resolveSelect[I](s: Select[RawName, I]): RState[Select[ResolvedName, I]] =
    for {
      from       <- s.from.traverse(resolveFrom)
      selectCols <- s.select.cols.traverse(resolveSelection)
    } yield Select(s.info, SelectCols(s.select.info, selectCols), from)

  def resolveOneCTE[I](cte: CTE[RawName, I]): RState[CTE[ResolvedName, I]] =
    for {
      query    <- resolveQuery(cte.q)
      colNames <- getQueryColumnNames(query)
      // Update the state with the column names from the query
      _ <- State.modify[Resolver](c => c.addCTE(cte.alias.value, colNames.toSet))

      // Update state to have no relations or columns in scope so we don't over resolve
      _ <- State.modify[Resolver](r => r.resetRelationScope)
      // TODO support Alias
    } yield CTE(cte.info, cte.alias, cte.cols, query)

  def resQueryWith[I](qw: QueryWith[RawName, I]): RState[Query[ResolvedName, I]] =
    for {
      ctes  <- qw.ctes.traverse(resolveOneCTE)
      query <- resolveQuery(qw.q)
    } yield QueryWith(qw.info, ctes, query)

  def resolveQuery[I](q: Query[RawName, I]): RState[Query[ResolvedName, I]] = q match {
    case QuerySelect(i, qs) =>
      resolveSelect(qs).map { s =>
        QuerySelect(i, s)
      }
    case QueryLimit(i, limit, qs) =>
      resolveSelect(qs).map { s =>
        QueryLimit(i, limit, s)
      }
    case QueryWith(i, ctes, q) => resQueryWith(QueryWith(i, ctes, q))
  }

}
