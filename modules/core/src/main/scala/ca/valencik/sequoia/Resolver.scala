package ca.valencik.sequoia

import cats.data.{Chain, EitherT, ReaderWriterState}
import cats.implicits._

sealed trait RawName extends Product with Serializable {
  def value: String
}
final case class RawTableName(value: String)  extends RawName
final case class RawColumnName(value: String) extends RawName

sealed trait ResolvedName extends Product with Serializable {
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
  // TODO this unsafe get feels wrong
  def addAllColumnsFromRelationToProjection(rel: String): Resolver =
    this.copy(s = t.get(rel).get ::: s)
  def addAllColumnsToProjection: Resolver = this.copy(s = t.values.flatten.toList ::: s)
  // TODO why is the "previous column" at the head?
  def addColumnAlias(alias: String): Resolver             = this.copy(s = alias :: s)
  def aliasPreviousColumnInScope(alias: String): Resolver = this.copy(s = alias :: s.tail)
  // NEXT TIME: I now believe we should handle `_col` naming as a seperate step from name resolution
  // Furthermore it's likely time we refactor Resolve to be simpler, we also need some way to say where in the SQL
  // source code we ran into an issue, i.e. linking the AST node type I info.
  def assignColAlias(): Resolver = this.copy(s = s"_col${s.length}" :: s)
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

  def logUnresolvedUsingColumn[I](
      column: UsingColumn[I, RawName],
      state: Resolver,
      catalog: Catalog
  ): String = {
    val col = column.value.value
    val rs  = state.r.mkString(",")
    val c   = catalog.c.toString
    s"Column '${col}' in 'JOIN USING' clause was not resolvable with relations: '${rs}' in catalog '${c}'"
  }

  def preserveScope[A](modify: => EitherRes[A]): EitherRes[A] =
    for {
      old   <- EitherT.right(ReaderWriterState.get[Catalog, Log, Resolver])
      value <- modify
      _     <- EitherT.right(ReaderWriterState.set[Catalog, Log, Resolver](old))
    } yield value

  def resolveTableRef[I](
      tr: TableRef[I, RawName]
  ): EitherRes[TableRef[I, ResolvedName]] =
    EitherT(ReaderWriterState { (cat, res) =>
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
    })

  def resolveColumnRef[I](
      col: ColumnRef[I, RawName]
  ): EitherRes[ColumnRef[I, ResolvedName]] =
    EitherT(ReaderWriterState { (cat, res) =>
      if (res.columnIsInScope(col.value))
        (
          Chain(s"Resolved Column '${col.value.value}'"),
          res.addColumnToProjection(col.value.value),
          Right(ColumnRef(col.info, ResolvedColumnName(col.value.value)))
        )
      else
        (Chain(logUnresolvedColumn(col, res, cat)), res, Left(ResolutionError(col.value)))
    })

  def resolveUsingColumn[I](uc: UsingColumn[I, RawName]): EitherRes[UsingColumn[I, ResolvedName]] =
    EitherT(ReaderWriterState { (cat, res) =>
      if (res.columnIsInScope(uc.value))
        (
          Chain(s"Resolved UsingColumn '${uc.value.value}'"),
          res,
          Right(UsingColumn(uc.info, ResolvedColumnName(uc.value.value)))
        )
      else
        (Chain(logUnresolvedUsingColumn(uc, res, cat)), res, Left(ResolutionError(uc.value)))
    })

  def resolveQuery[I](
      q: Query[I, RawName]
  ): EitherRes[Query[I, ResolvedName]] =
    for {
      w   <- q.cte.traverse(resolveWith)
      qnw <- resolveQueryNoWith(q.queryNoWith)
    } yield Query(q.info, w, qnw)

  def resolveWith[I](
      w: With[I, RawName]
  ): EitherRes[With[I, ResolvedName]] =
    for {
      nqs <- w.namedQueries.traverse(resolveNamedQuery)
    } yield With(w.info, nqs)

  def resolveNamedQuery[I](
      nq: NamedQuery[I, RawName]
  ): EitherRes[NamedQuery[I, ResolvedName]] =
    for {
      q <- resolveQuery(nq.query)
      _ <- EitherT.right(
        // Update Resolver with CTE and reset so we have an empty scope for the next query
        ReaderWriterState.modify[Catalog, Log, Resolver](_.addCTE(nq.name).resetRelationScope())
      )
      // TODO column aliases
    } yield NamedQuery(nq.info, nq.name, None, q)

  def resolveOrderBy[I](ob: OrderBy[I, RawName]): EitherRes[OrderBy[I, ResolvedName]] =
    for {
      sis <- ob.sortItems.traverse(resolveSortItem)
    } yield OrderBy(ob.info, sis)

  def resolveSortItem[I](si: SortItem[I, RawName]): EitherRes[SortItem[I, ResolvedName]] =
    for {
      e <- resolveExpression(si.exp)
    } yield SortItem(si.info, e, si.ordering, si.nullOrdering)

  def resolveQueryNoWith[I](
      qnw: QueryNoWith[I, RawName]
  ): EitherRes[QueryNoWith[I, ResolvedName]] =
    for {
      qt <- resolveQueryTerm(qnw.queryTerm)
      ob <- qnw.orderBy.traverse(resolveOrderBy)
    } yield QueryNoWith(qnw.info, qt, ob, qnw.limit)

  def resolveQueryTerm[I](
      qt: QueryTerm[I, RawName]
  ): EitherRes[QueryTerm[I, ResolvedName]] =
    qt match {
      case qp: QueryPrimary[I, RawName] => resolveQueryPrimary(qp).widen
      case so: SetOperation[I, RawName] => resolveSetOperation(so).widen
    }

  def resolveSetOperation[I](
      so: SetOperation[I, RawName]
  ): EitherRes[SetOperation[I, ResolvedName]] =
    for {
      left  <- resolveQueryTerm(so.left)
      right <- resolveQueryTerm(so.right)
    } yield SetOperation(so.info, left, so.op, so.setQuantifier, right)

  def resolveQueryPrimary[I](
      qp: QueryPrimary[I, RawName]
  ): EitherRes[QueryPrimary[I, ResolvedName]] =
    qp match {
      case qs: QuerySpecification[I, RawName] => resolveQuerySpecification(qs).widen
      case qp: QueryPrimaryTable[I, RawName]  => resolveQueryPrimaryTable(qp).widen
      case it: InlineTable[I, RawName]        => resolveInlineTable(it).widen
      case sq: SubQuery[I, RawName]           => resolveSubQuery(sq).widen
    }

  // TODO Does this need and state preservation business?
  def resolveQueryPrimaryTable[I](
      qpt: QueryPrimaryTable[I, RawName]
  ): EitherRes[QueryPrimaryTable[I, ResolvedName]] =
    for {
      tr <- resolveTableRef(qpt.table)
    } yield QueryPrimaryTable(qpt.info, tr)

  def resolveInlineTable[I](it: InlineTable[I, RawName]): EitherRes[InlineTable[I, ResolvedName]] =
    for {
      es <- it.values.traverse(resolveExpression)
    } yield InlineTable(it.info, es)

  def resolveSubQuery[I](sq: SubQuery[I, RawName]): EitherRes[SubQuery[I, ResolvedName]] =
    for {
      qnw <- resolveQueryNoWith(sq.queryNoWith)
    } yield SubQuery(sq.info, qnw)

  def resolveQuerySpecification[I](
      qs: QuerySpecification[I, RawName]
  ): EitherRes[QuerySpecification[I, ResolvedName]] =
    for {
      from <- qs.from.traverse(resolveRelation)
      sis  <- qs.selectItems.traverse(resolveSelectItem)
      w    <- qs.where.traverse(resolveExpression)
      g    <- qs.groupBy.traverse(resolveGroupBy)
      h    <- qs.having.traverse(resolveExpression)
    } yield QuerySpecification(qs.info, qs.setQuantifier, sis, from, w, g, h)

  def resolveGroupBy[I](gb: GroupBy[I, RawName]): EitherRes[GroupBy[I, ResolvedName]] =
    for {
      ges <- gb.groupingElements.traverse(resolveGroupingElement)
    } yield GroupBy(gb.info, gb.setQuantifier, ges)

  def resolveGroupingElement[I](
      ge: GroupingElement[I, RawName]
  ): EitherRes[GroupingElement[I, ResolvedName]] =
    ge match {
      case sgs: SingleGroupingSet[I, RawName]    => resolveSingleGroupSet(sgs).widen
      case r: Rollup[I, RawName]                 => resolveRollup(r).widen
      case c: Cube[I, RawName]                   => resolveCube(c).widen
      case mgs: MultipleGroupingSets[I, RawName] => resolveMultipleGroupingSets(mgs).widen
    }

  def resolveSingleGroupSet[I](
      sgs: SingleGroupingSet[I, RawName]
  ): EitherRes[SingleGroupingSet[I, ResolvedName]] =
    for {
      g <- resolveGroupSet(sgs.groupingSet)
    } yield SingleGroupingSet(sgs.info, g)

  def resolveRollup[I](c: Rollup[I, RawName]): EitherRes[Rollup[I, ResolvedName]] =
    for {
      es <- c.expressions.traverse(resolveExpression)
    } yield Rollup(c.info, es)

  def resolveCube[I](c: Cube[I, RawName]): EitherRes[Cube[I, ResolvedName]] =
    for {
      es <- c.expressions.traverse(resolveExpression)
    } yield Cube(c.info, es)

  def resolveMultipleGroupingSets[I](
      mgs: MultipleGroupingSets[I, RawName]
  ): EitherRes[MultipleGroupingSets[I, ResolvedName]] =
    for {
      gs <- mgs.groupingSets.traverse(resolveGroupSet)
    } yield MultipleGroupingSets(mgs.info, gs)

  def resolveGroupSet[I](g: GroupingSet[I, RawName]): EitherRes[GroupingSet[I, ResolvedName]] =
    for {
      es <- g.expressions.traverse(resolveExpression)
    } yield GroupingSet(g.info, es)

  def resolveSelectItem[I](
      rel: SelectItem[I, RawName]
  ): EitherRes[SelectItem[I, ResolvedName]] =
    rel match {
      case sa: SelectAll[I, RawName]    => resolveSelectAll(sa).widen
      case ss: SelectSingle[I, RawName] => resolveSelectSingle(ss).widen
    }

  def resolveSelectAll[I](
      sa: SelectAll[I, RawName]
  ): EitherRes[SelectAll[I, ResolvedName]] =
    for {
      ref <- preserveScope(sa.ref.traverse(resolveTableRef))
      _ <- EitherT.right(
        ReaderWriterState.modify[Catalog, Log, Resolver] {
          case res => {
            ref match {
              case None     => res.addAllColumnsToProjection
              case Some(tr) => res.addAllColumnsFromRelationToProjection(tr.value.value)
            }
          }
        }
      )
    } yield SelectAll(sa.info, ref, sa.aliases)

  def resolveSelectSingle[I](
      ss: SelectSingle[I, RawName]
  ): EitherRes[SelectSingle[I, ResolvedName]] =
    for {
      e <- resolveExpression(ss.expr)
      _ <- EitherT.right(resolveColumnAlias(e, ss.alias))
    } yield SelectSingle(ss.info, e, ss.alias)

  // def resolveColumnAlias[I](
  //     exp: Expression[I, ResolvedName],
  //     colAlias: Option[ColumnAlias[I]]
  // ): EitherRes[Unit] =
  //   EitherT(ReaderWriterState { (_, res) =>
  //     (exp, colAlias) match {
  //       case (_: ColumnExpr[I, ResolvedName], Some(alias)) =>
  //         (
  //           Chain(s"Aliasing previous column '${res.s.head}' to '${alias.value}'"),
  //           res.aliasPreviousColumnInScope(alias.value),
  //           Right(())
  //         )
  //       case (_: ColumnExpr[I, ResolvedName], None) =>
  //         (
  //           Chain(s"No alias for column '${res.s.head}'"),
  //           res,
  //           Right(())
  //         )
  //       case (_, Some(alias)) =>
  //         (
  //           Chain(s"Expr with alias '${alias.value}' added to projection"),
  //           res.addColumnAlias(alias.value),
  //           Right(())
  //         )
  //       case (_, None) =>
  //         (
  //           Chain(s"No alias for expr, assigning one"),
  //           res.assignColAlias(),
  //           Right(())
  //         )
  //     }
  //   })

  def resolveColumnAlias[I](
      exp: Expression[I, ResolvedName],
      colAlias: Option[ColumnAlias[I]]
  ): RState[Unit] =
    ReaderWriterState.modify[Catalog, Log, Resolver] { res =>
      (exp, colAlias) match {
        case (_: ColumnExpr[I, ResolvedName], Some(alias)) =>
          res.aliasPreviousColumnInScope(alias.value)
        case (_: ColumnExpr[I, ResolvedName], None) =>
          res
        case (_, Some(alias)) =>
          res.addColumnAlias(alias.value)
        case (_, None) =>
          res.assignColAlias()
      }
    }

  def resolveExpression[I](expr: Expression[I, RawName]): EitherRes[Expression[I, ResolvedName]] =
    expr match {
      case lb: LogicalBinary[I, RawName]   => resolveLogicalBinary(lb).widen
      case p: Predicate[I, RawName]        => resolvePredicate(p).widen
      case ve: ValueExpression[I, RawName] => resolveValueExpression(ve).widen
    }

  def resolveLogicalBinary[I](
      lb: LogicalBinary[I, RawName]
  ): EitherRes[LogicalBinary[I, ResolvedName]] =
    for {
      left <- resolveExpression(lb.left)
      // TODO: Is the left allowed to modify the resolver state? It currently does...
      right <- resolveExpression(lb.right)
    } yield LogicalBinary(lb.info, left, lb.op, right)

  def resolvePredicate[I](
      pred: Predicate[I, RawName]
  ): EitherRes[Predicate[I, ResolvedName]] =
    pred match {
      case e: NotPredicate[I, RawName] =>
        resolveExpression(e.exp).map(NotPredicate(e.info, _)).widen
      case e: ComparisonExpr[I, RawName]       => resolveComparisonExpr(e).widen
      case e: QuantifiedComparison[I, RawName] => resolveQuantifiedComparison(e).widen
      case e: Between[I, RawName]              => resolveBetween(e).widen
      case e: InList[I, RawName]               => resolveInList(e).widen
      case e: InSubQuery[I, RawName]           => resolveInSubQuery(e).widen
      case e: Like[I, RawName]                 => resolveLike(e).widen
      case e: NullPredicate[I, RawName]        => resolveNullPredicate(e).widen
      case e: DistinctFrom[I, RawName]         => resolveDistinctFrom(e).widen
    }

  def resolveComparisonExpr[I](
      e: ComparisonExpr[I, RawName]
  ): EitherRes[ComparisonExpr[I, ResolvedName]] =
    for {
      left <- resolveValueExpression(e.left)
      // TODO: Is the left allowed to modify the resolver state? It currently does...
      right <- resolveValueExpression(e.right)
    } yield ComparisonExpr(e.info, left, e.op, right)

  def resolveQuantifiedComparison[I](
      e: QuantifiedComparison[I, RawName]
  ): EitherRes[QuantifiedComparison[I, ResolvedName]] =
    for {
      value <- resolveValueExpression(e.value)
      query <- resolveQuery(e.query)
    } yield QuantifiedComparison(e.info, value, e.op, e.quantifier, query)

  def resolveBetween[I](
      e: Between[I, RawName]
  ): EitherRes[Between[I, ResolvedName]] =
    for {
      // TODO: preserve scope?
      value <- preserveScope(resolveValueExpression(e.value))
      lower <- preserveScope(resolveValueExpression(e.lower))
      upper <- preserveScope(resolveValueExpression(e.upper))
    } yield Between(e.info, value, lower, upper)

  def resolveInList[I](
      e: InList[I, RawName]
  ): EitherRes[InList[I, ResolvedName]] =
    for {
      value <- resolveValueExpression(e.value)
      exps  <- e.exps.traverse(resolveExpression)
    } yield InList(e.info, value, exps)

  def resolveInSubQuery[I](
      e: InSubQuery[I, RawName]
  ): EitherRes[InSubQuery[I, ResolvedName]] =
    for {
      value <- resolveValueExpression(e.value)
      query <- resolveQuery(e.query)
    } yield InSubQuery(e.info, value, query)

  def resolveLike[I](
      e: Like[I, RawName]
  ): EitherRes[Like[I, ResolvedName]] =
    for {
      value   <- resolveValueExpression(e.value)
      pattern <- resolveValueExpression(e.pattern)
      escape  <- e.escape.traverse(resolveValueExpression)
    } yield Like(e.info, value, pattern, escape)

  def resolveNullPredicate[I](
      e: NullPredicate[I, RawName]
  ): EitherRes[NullPredicate[I, ResolvedName]] =
    for {
      value <- resolveValueExpression(e.value)
    } yield NullPredicate(e.info, value)

  def resolveDistinctFrom[I](
      e: DistinctFrom[I, RawName]
  ): EitherRes[DistinctFrom[I, ResolvedName]] =
    for {
      value <- resolveValueExpression(e.value)
      right <- resolveValueExpression(e.right)
    } yield DistinctFrom(e.info, value, right)

  def resolveValueExpression[I](
      expr: ValueExpression[I, RawName]
  ): EitherRes[ValueExpression[I, ResolvedName]] =
    expr match {
      case e: PrimaryExpression[I, RawName] => resolvePrimaryExpression(e).widen
      case e: ArithmeticUnary[I, RawName] =>
        resolveValueExpression(e.value).map(ArithmeticUnary(e.info, e.sign, _))
      case e: ArithmeticBinary[I, RawName] => resolveArithmeticBinary(e).widen
    }

  def resolveArithmeticBinary[I](
      ab: ArithmeticBinary[I, RawName]
  ): EitherRes[ArithmeticBinary[I, ResolvedName]] =
    for {
      left <- resolveValueExpression(ab.left)
      // TODO: Is the left allowed to modify the resolver state? It currently does...
      right <- resolveValueExpression(ab.right)
    } yield ArithmeticBinary(ab.info, left, ab.op, right)

  def resolvePrimaryExpression[I](
      expr: PrimaryExpression[I, RawName]
  ): EitherRes[PrimaryExpression[I, ResolvedName]] =
    expr match {
      case e: ColumnExpr[I, RawName]   => resolveColumnExpr(e).widen
      case e: LiteralExpr[I, RawName]  => resolveLiteralExpr(e).widen
      case e: SubQueryExpr[I, RawName] => resolveSubQueryExpr(e).widen
      case e: ExistsExpr[I, RawName]   => resolveExistsExpr(e).widen
      case _                           => ???
    }

  def resolveLiteralExpr[I](
      expr: LiteralExpr[I, RawName]
  ): EitherRes[LiteralExpr[I, ResolvedName]] =
    EitherT.right(
      ReaderWriterState
        .pure[Catalog, Log, Resolver, LiteralExpr[I, ResolvedName]](
          // We can simply cast it as LiteralExpr doesn't contain a RawName,
          // it is a phantom type.
          // TODO: I wonder if there's a better way
          expr.asInstanceOf[LiteralExpr[I, ResolvedName]]
        )
    )

  def resolveExistsExpr[I](
      expr: ExistsExpr[I, RawName]
  ): EitherRes[ExistsExpr[I, ResolvedName]] =
    for {
      // ExistsExpr cannot bring columns and relations into scope
      q <- preserveScope(resolveQuery(expr.query))
    } yield ExistsExpr(expr.info, q)

  def resolveSubQueryExpr[I](
      expr: SubQueryExpr[I, RawName]
  ): EitherRes[SubQueryExpr[I, ResolvedName]] =
    for {
      // SubQueryExpr cannot bring columns and relations into scope
      q <- preserveScope(resolveQuery(expr.query))
    } yield SubQueryExpr(expr.info, q)

  def resolveColumnExpr[I](ce: ColumnExpr[I, RawName]): EitherRes[ColumnExpr[I, ResolvedName]] =
    for {
      cr <- resolveColumnRef(ce.col)
    } yield ColumnExpr(ce.info, cr)

  def resolveRelation[I](rel: Relation[I, RawName]): EitherRes[Relation[I, ResolvedName]] =
    rel match {
      case sr: SampledRelation[I, RawName] => resolveSampledRelation(sr).widen
      case jr: JoinRelation[I, RawName]    => resolveJoinRelation(jr).widen
    }

  def resolveSampledRelation[I](
      sr: SampledRelation[I, RawName]
  ): EitherRes[SampledRelation[I, ResolvedName]] =
    for {
      ar <- resolveAliasedRelation(sr.aliasedRelation)
      ts <- sr.tableSample.traverse(resolveTableSample)
    } yield SampledRelation(sr.info, ar, ts)

  def resolveTableSample[I](
      ts: TableSample[I, RawName]
  ): EitherRes[TableSample[I, ResolvedName]] =
    for {
      e <- resolveExpression(ts.percentage)
    } yield TableSample(ts.info, ts.sampleType, e)

  def resolveAliasedRelation[I](
      ar: AliasedRelation[I, RawName]
  ): EitherRes[AliasedRelation[I, ResolvedName]] =
    for {
      rp <- resolveRelationPrimary(ar.relationPrimary)
      // TODO: TableAlias and ColumnAliases
    } yield AliasedRelation(ar.info, rp, None, None)

  def resolveRelationPrimary[I](
      rel: RelationPrimary[I, RawName]
  ): EitherRes[RelationPrimary[I, ResolvedName]] =
    rel match {
      case tn: TableName[I, RawName]             => resolveTableName(tn).widen
      case sqr: SubQueryRelation[I, RawName]     => resolveSubQueryRelation(sqr).widen
      case un: Unnest[I, RawName]                => resolveUnnest(un).widen
      case lr: LateralRelation[I, RawName]       => resolveLateralRelation(lr).widen
      case pr: ParenthesizedRelation[I, RawName] => resolveParenthesizedRelation(pr).widen
    }

  def resolveTableName[I](tn: TableName[I, RawName]): EitherRes[TableName[I, ResolvedName]] =
    for {
      tr <- resolveTableRef(tn.ref)
    } yield TableName(tn.info, tr)

  def resolveSubQueryRelation[I](
      sqr: SubQueryRelation[I, RawName]
  ): EitherRes[SubQueryRelation[I, ResolvedName]] =
    for {
      q <- resolveQuery(sqr.query)
    } yield SubQueryRelation(sqr.info, q)

  def resolveUnnest[I](un: Unnest[I, RawName]): EitherRes[Unnest[I, ResolvedName]] =
    for {
      es <- un.expressions.traverse(resolveExpression)
    } yield Unnest(un.info, es, un.ordinality)

  def resolveLateralRelation[I](
      lr: LateralRelation[I, RawName]
  ): EitherRes[LateralRelation[I, ResolvedName]] =
    for {
      q <- resolveQuery(lr.query)
    } yield LateralRelation(lr.info, q)

  def resolveParenthesizedRelation[I](
      pr: ParenthesizedRelation[I, RawName]
  ): EitherRes[ParenthesizedRelation[I, ResolvedName]] =
    for {
      r <- resolveRelation(pr.relation)
    } yield ParenthesizedRelation(pr.info, r)

  def resolveJoinRelation[I](
      jr: JoinRelation[I, RawName]
  ): EitherRes[JoinRelation[I, ResolvedName]] =
    for {
      // TODO confirm ordering and scope rules
      left  <- resolveRelation(jr.left)
      right <- resolveRelation(jr.right)
      jc    <- jr.criteria.traverse(resolveJoinCriteria)
    } yield JoinRelation(jr.info, jr.jointype, left, right, jc)

  def resolveJoinCriteria[I](
      jc: JoinCriteria[I, RawName]
  ): EitherRes[JoinCriteria[I, ResolvedName]] =
    jc match {
      case jo: JoinOn[I, RawName]    => resolveJoinOn(jo).widen
      case ju: JoinUsing[I, RawName] => resolveJoinUsing(ju).widen
    }

  def resolveJoinOn[I](
      jo: JoinOn[I, RawName]
  ): EitherRes[JoinOn[I, ResolvedName]] =
    for {
      // Resolving a JoinOn expression shouldn't bring things into scope
      exp <- preserveScope(resolveExpression(jo.expr))
    } yield JoinOn(jo.info, exp)

  def resolveJoinUsing[I](
      ju: JoinUsing[I, RawName]
  ): EitherRes[JoinUsing[I, ResolvedName]] =
    for {
      // TODO Check out JoinUsing's scope rules
      ucs <- preserveScope(ju.cols.traverse(resolveUsingColumn))
    } yield JoinUsing(ju.info, ucs)

}
