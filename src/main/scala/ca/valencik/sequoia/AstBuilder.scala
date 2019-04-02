package ca.valencik.sequoia

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

import cats.data.NonEmptyList

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  type Info          = Int
  type RawExpression = Expression[Info, RawName]

  val verbose: Boolean = false

  val nextId = {
    var i = 0;
    () =>
      { i += 1; i }
  }

  def toUnsafeNEL[A](xs: Buffer[A]): NonEmptyList[A] = NonEmptyList.fromListUnsafe(xs.toList)

  def qualifiedNameAsString(ctx: SqlBaseParser.QualifiedNameContext): String = {
    val idents = ctx.identifier.asScala.map(visit(_).asInstanceOf[Identifier])
    idents.map(_.value).mkString(".")
  }

  def getColumnName(ctx: SqlBaseParser.QualifiedNameContext): RawName = {
    RawColumnName(qualifiedNameAsString(ctx))
  }

  def getColumnName(ctx: SqlBaseParser.IdentifierContext): RawName = {
    val ident = visit(ctx).asInstanceOf[Identifier]
    RawColumnName(ident.value)
  }

  def getTableName(ctx: SqlBaseParser.QualifiedNameContext): RawTableName = {
    RawTableName(qualifiedNameAsString(ctx))
  }

  def maybeSetQuantifier(ctx: SqlBaseParser.SetQuantifierContext): Option[SetQuantifier] = {
    if (ctx != null)
      Some(
        if (ctx.DISTINCT != null) DISTINCT else ALL
      )
    else None
  }

  def getJoinType(ctx: SqlBaseParser.JoinRelationContext): JoinType = {
    if (ctx.CROSS != null)
      CrossJoin
    else {
      val jt = ctx.joinType
      if (jt.LEFT != null)
        LeftJoin
      else if (jt.RIGHT != null)
        RightJoin
      else if (jt.FULL != null)
        FullJoin
      else
        InnerJoin
    }
  }

  def getJoinCriteria(
      ctx: SqlBaseParser.JoinRelationContext): Option[JoinCriteria[Info, RawName]] = {
    if (ctx.CROSS != null)
      None
    else {
      val jc = ctx.joinCriteria
      if (jc.ON != null) {
        Some(JoinOn(nextId(), visit(jc.booleanExpression).asInstanceOf[Expression[Info, RawName]]))
      } else if (jc.USING != null) {
        val ucs: List[UsingColumn[Info, RawName]] =
          ctx
            .joinCriteria()
            .identifier
            .asScala
            .map { case ic => UsingColumn(nextId(), getColumnName(ic)) }
            .toList
        Some(JoinUsing(nextId(), NonEmptyList.fromListUnsafe(ucs)))
      } else
        None
    }
  }

  def getRight(ctx: SqlBaseParser.JoinRelationContext): Relation[Info, RawName] = {
    if (ctx.CROSS != null)
      visitSampledRelation(ctx.right)
    else if (ctx.NATURAL != null)
      visitSampledRelation(ctx.right)
    else
      visit(ctx.rightRelation).asInstanceOf[Relation[Info, RawName]]
  }

  def getComparisonOperator(ctx: SqlBaseParser.ComparisonOperatorContext): Comparison = {
    if (ctx.EQ != null)
      EQ
    else if (ctx.NEQ != null)
      NEQ
    else if (ctx.GT != null)
      GT
    else if (ctx.LT != null)
      LT
    else if (ctx.GTE != null)
      GTE
    else if (ctx.LTE != null)
      LTE
    else
      ???
  }

  // -- Overrides
  override def visitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): Node = {
    visit(ctx.statement)
  }

  override def visitQuery(ctx: SqlBaseParser.QueryContext): Query[Info, RawName] = {
    if (verbose) println(s"-------visitQuery called: ${ctx.getText}-------------")
    val maybeWith = if (ctx.`with` != null) Some(visitWith(ctx.`with`)) else None
    Query(nextId(), maybeWith, visitQueryNoWith(ctx.queryNoWith))
  }

  override def visitWith(ctx: SqlBaseParser.WithContext): With[Info, RawName] = {
    if (verbose) println(s"-------visitWith called: ${ctx.getText}-------------")
    val nqs = toUnsafeNEL(ctx.namedQuery.asScala.map(visitNamedQuery))
    With(nextId(), nqs)
  }

  override def visitQueryNoWith(
      ctx: SqlBaseParser.QueryNoWithContext): QueryNoWith[Info, RawName] = {
    if (verbose) println(s"-------visitQueryNoWith called: ${ctx.getText}-------------")
    val qt = visit(ctx.queryTerm).asInstanceOf[QueryTerm[Info, RawName]]
    val orderBy = {
      if (ctx.sortItem.size > 0)
        Some(OrderBy(nextId(), toUnsafeNEL(ctx.sortItem.asScala.map(visitSortItem))))
      else
        None
    }
    val limit = if (ctx.LIMIT != null) Some(Limit(nextId(), ctx.limit.getText)) else None
    QueryNoWith(nextId(), qt, orderBy, limit)
  }

  override def visitSetOperation(
      ctx: SqlBaseParser.SetOperationContext): SetOperation[Info, RawName] = {
    val left  = visit(ctx.left).asInstanceOf[QueryTerm[Info, RawName]]
    val right = visit(ctx.right).asInstanceOf[QueryTerm[Info, RawName]]
    val sq    = maybeSetQuantifier(ctx.setQuantifier)
    val op = ctx.operator.getType match {
      case SqlBaseLexer.INTERSECT => INTERSECT
      case SqlBaseLexer.UNION     => UNION
      case SqlBaseLexer.EXCEPT    => EXCEPT
    }
    SetOperation(nextId(), left, op, sq, right)
  }

  override def visitSortItem(ctx: SqlBaseParser.SortItemContext): SortItem[Info, RawName] = {
    if (verbose) println(s"-------visitSortItem called: ${ctx.getText}-------------")
    // TODO
    val exp = visitExpression(ctx.expression).asInstanceOf[Expression[Info, RawName]]
    val o =
      if (ctx.ordering != null)
        Some(
          if (ctx.ASC != null) ASC else DESC
        )
      else None
    val no =
      if (ctx.nullOrdering != null)
        Some(
          if (ctx.FIRST != null) FIRST else LAST
        )
      else None
    SortItem(nextId(), exp, o, no)
  }

  override def visitQuerySpecification(
      ctx: SqlBaseParser.QuerySpecificationContext): QuerySpecification[Info, RawName] = {
    if (verbose) println(s"-------visitQuerySpecification called: ${ctx.getText}-------------")
    // TODO use maybeSetQuantifier
    val sq = maybeSetQuantifier(ctx.setQuantifier)
    val sis = toUnsafeNEL(
      ctx.selectItem.asScala.map(visit(_).asInstanceOf[SelectItem[Info, RawName]]))
    val f = NonEmptyList.fromList(
      ctx.relation.asScala.map(visit(_).asInstanceOf[Relation[Info, RawName]]).toList)
    // TODO SetQuantifier
    val w = if (ctx.where != null) Some(visit(ctx.where).asInstanceOf[RawExpression]) else None
    val g =
      if (ctx.groupBy != null) Some(visit(ctx.groupBy).asInstanceOf[GroupBy[Info, RawName]])
      else None
    val h = if (ctx.having != null) Some(visit(ctx.having).asInstanceOf[RawExpression]) else None

    QuerySpecification(nextId(), sq, sis, f, w, g, h)
  }

  override def visitGroupBy(ctx: SqlBaseParser.GroupByContext): GroupBy[Info, RawName] = {
    val sq = maybeSetQuantifier(ctx.setQuantifier)
    val ges = toUnsafeNEL(ctx.groupingElement.asScala.map { g =>
      visit(g).asInstanceOf[GroupingElement[Info, RawName]]
    })
    GroupBy(nextId(), sq, ges)
  }

  override def visitSingleGroupingSet(
      ctx: SqlBaseParser.SingleGroupingSetContext): GroupingElement[Info, RawName] = {
    SingleGroupingSet(nextId(), visit(ctx.groupingSet).asInstanceOf[GroupingSet[Info, RawName]])
  }

  override def visitGroupingSet(
      ctx: SqlBaseParser.GroupingSetContext): GroupingSet[Info, RawName] = {
    val es = ctx.expression.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    GroupingSet(nextId(), es)
  }

  override def visitNamedQuery(ctx: SqlBaseParser.NamedQueryContext): NamedQuery[Info, RawName] = {
    if (verbose) println(s"-------visitNamedQuery called: ${ctx.getText}-------------")
    val query = visitQuery(ctx.query)
    val colAs =
      if (ctx.columnAliases != null)
        Some(visitColumnAliases(ctx.columnAliases))
      else
        None
    NamedQuery(nextId(), ctx.name.getText, colAs, query)
  }

  override def visitSelectSingle(
      ctx: SqlBaseParser.SelectSingleContext): SelectSingle[Info, RawName] = {
    val ident = if (ctx.identifier != null) Some(ctx.identifier.getText) else None
    val alias: Option[ColumnAlias[Info]] = ident.map { a =>
      ColumnAlias(nextId(), a)
    }
    val expr = visit(ctx.expression).asInstanceOf[RawExpression]
    SelectSingle(nextId(), expr, alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext): SelectAll[Info, RawName] = {
    val ref: Option[TableRef[Info, RawName]] =
      if (ctx.qualifiedName != null) Some(TableRef(nextId(), getTableName(ctx.qualifiedName)))
      else None
    SelectAll(nextId(), ref)
  }

  override def visitJoinRelation(
      ctx: SqlBaseParser.JoinRelationContext): JoinRelation[Info, RawName] = {
    val left         = visit(ctx.left).asInstanceOf[Relation[Info, RawName]]
    val right        = getRight(ctx)
    val joinType     = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    JoinRelation(nextId(), joinType, left, right, joinCriteria)
  }

  override def visitSampledRelation(
      ctx: SqlBaseParser.SampledRelationContext): SampledRelation[Info, RawName] = {
    val ar = visitAliasedRelation(ctx.aliasedRelation)
    val ts = if (ctx.sampleType != null) {
      val st = if (ctx.sampleType.BERNOULLI != null) BERNOULLI else SYSTEM
      val p  = visit(ctx.percentage).asInstanceOf[RawExpression]
      Some(TableSample(nextId(), st, p))
    } else None
    SampledRelation(nextId(), ar, ts)
  }

  override def visitAliasedRelation(
      ctx: SqlBaseParser.AliasedRelationContext): AliasedRelation[Info, RawName] = {
    val rp = visit(ctx.relationPrimary).asInstanceOf[RelationPrimary[Info, RawName]]
    val alias =
      if (ctx.identifier != null) Some(TableAlias(nextId(), ctx.identifier.getText)) else None
    val colAs =
      if (ctx.columnAliases != null)
        Some(visitColumnAliases(ctx.columnAliases))
      else
        None
    AliasedRelation(nextId(), rp, alias, colAs)
  }

  override def visitColumnAliases(ctx: SqlBaseParser.ColumnAliasesContext): ColumnAliases[Info] = {
    val cols = toUnsafeNEL(ctx.identifier.asScala.map { i =>
      ColumnAlias(nextId(), visit(i).asInstanceOf[Identifier].value)
    })
    ColumnAliases(cols)
  }

  override def visitTableName(
      ctx: SqlBaseParser.TableNameContext): RelationPrimary[Info, RawName] = {
    val ref: TableRef[Info, RawName] = TableRef(nextId(), getTableName(ctx.qualifiedName))
    TableName(nextId(), ref)
  }

  override def visitSubqueryRelation(
      ctx: SqlBaseParser.SubqueryRelationContext): RelationPrimary[Info, RawName] = {
    SubQueryRelation(nextId(), visit(ctx.query).asInstanceOf[Query[Info, RawName]])
  }

  override def visitUnnest(ctx: SqlBaseParser.UnnestContext): RelationPrimary[Info, RawName] = ???

  override def visitLateral(ctx: SqlBaseParser.LateralContext): RelationPrimary[Info, RawName] = {
    val q = visitQuery(ctx.query)
    LateralRelation(nextId(), q)
  }

  override def visitParenthesizedRelation(
      ctx: SqlBaseParser.ParenthesizedRelationContext): RelationPrimary[Info, RawName] = {
    val r = visit(ctx.relation).asInstanceOf[Relation[Info, RawName]]
    ParenthesizedRelation(nextId(), r)
  }

  override def visitPredicated(ctx: SqlBaseParser.PredicatedContext): Node = {
    if (verbose) println(s"-------visitPredicated called: ${ctx.getText}-------------")
    if (ctx.predicate != null) visit(ctx.predicate) else visit(ctx.valueExpression)
  }

  override def visitDereference(ctx: SqlBaseParser.DereferenceContext): Node = {
    if (verbose) println(s"-------visitDereference called: ${ctx.getText}-------------")
    val base      = visit(ctx.base).asInstanceOf[RawExpression]
    val fieldName = visit(ctx.fieldName).asInstanceOf[Identifier]
    DereferenceExpr(nextId(), base, fieldName.value)
  }

  override def visitSubqueryExpression(
      ctx: SqlBaseParser.SubqueryExpressionContext): SubQueryExpr[Info, RawName] = {
    if (verbose) println(s"-------visitSubqueryExpression called: ${ctx.getText}-------------")
    SubQueryExpr(nextId(), visitQuery(ctx.query))
  }

  override def visitComparison(
      ctx: SqlBaseParser.ComparisonContext): ComparisonExpr[Info, RawName] = {
    if (verbose) println(s"-------visitComparison called: ${ctx.getText}-------------")
    val op    = getComparisonOperator(ctx.comparisonOperator)
    val left  = visit(ctx.value).asInstanceOf[RawExpression]
    val right = visit(ctx.right).asInstanceOf[RawExpression]
    ComparisonExpr(nextId(), left, op, right)
  }

  override def visitLogicalBinary(
      ctx: SqlBaseParser.LogicalBinaryContext): BooleanExpr[Info, RawName] = {
    val op    = if (ctx.AND != null) AND else OR
    val left  = visit(ctx.left).asInstanceOf[RawExpression]
    val right = visit(ctx.right).asInstanceOf[RawExpression]
    BooleanExpr(nextId(), left, op, right)
  }

  override def visitColumnReference(
      ctx: SqlBaseParser.ColumnReferenceContext): ColumnExpr[Info, RawName] = {
    if (verbose) println(s"-------visitColumnReference called: ${ctx.getText}-------------")
    ColumnExpr(nextId(), ColumnRef(nextId(), getColumnName(ctx.identifier)))
  }

  override def visitFunctionCall(
      ctx: SqlBaseParser.FunctionCallContext): FunctionCall[Info, RawName] = {
    if (verbose) println(s"-------visitFunctionCall called: ${ctx.getText}-------------")
    val name  = qualifiedNameAsString(ctx.qualifiedName)
    val sq    = maybeSetQuantifier(ctx.setQuantifier)
    val exprs = ctx.expression.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    val orderby = {
      if (ctx.sortItem.size > 0)
        Some(OrderBy(nextId(), toUnsafeNEL(ctx.sortItem.asScala.map(visitSortItem))))
      else
        None
    }
    val filter = if (ctx.filter != null) Some(visitFilter(ctx.filter)) else None
    val over   = if (ctx.over != null) Some(visitOver(ctx.over)) else None

    FunctionCall(nextId(), name, sq, exprs, orderby, filter, over)
  }

  override def visitFilter(ctx: SqlBaseParser.FilterContext): FunctionFilter[Info, RawName] = {
    if (verbose) println(s"-------visitFunctionFilter called: ${ctx.getText}-------------")
    val exp = visit(ctx.booleanExpression).asInstanceOf[BooleanExpr[Info, RawName]]
    FunctionFilter(nextId(), exp)
  }

  override def visitOver(ctx: SqlBaseParser.OverContext): FunctionOver[Info, RawName] = {
    if (verbose) println(s"-------visitFunctionOver called: ${ctx.getText}-------------")
    val partitionBy = ctx.partition.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    val orderBy = {
      if (ctx.sortItem.size > 0)
        Some(OrderBy(nextId(), toUnsafeNEL(ctx.sortItem.asScala.map(visitSortItem))))
      else
        None
    }
    val window = if (ctx.windowFrame != null) Some(visitWindowFrame(ctx.windowFrame)) else None
    FunctionOver(nextId(), partitionBy, orderBy, window)
  }

  override def visitWindowFrame(
      ctx: SqlBaseParser.WindowFrameContext): WindowFrame[Info, RawName] = {
    if (verbose) println(s"-------visitWindowFrame called: ${ctx.getText}-------------")
    val ft = ctx.frameType.getType match {
      case SqlBaseLexer.RANGE => RANGE
      case SqlBaseLexer.ROWS  => ROWS
    }
    val start = visit(ctx.start).asInstanceOf[FrameBound[Info, RawName]]
    val end =
      if (ctx.end != null) Some(visit(ctx.end).asInstanceOf[FrameBound[Info, RawName]]) else None
    WindowFrame(nextId(), ft, start, end)
  }

  override def visitUnboundedFrame(
      ctx: SqlBaseParser.UnboundedFrameContext): UnboundedFrame[Info, RawName] = {
    if (verbose) println(s"-------visitUnboundedFrame called: ${ctx.getText}-------------")
    val bound = ctx.boundType.getType match {
      case SqlBaseLexer.PRECEDING => PRECEDING
      case SqlBaseLexer.FOLLOWING => FOLLOWING
    }
    UnboundedFrame(nextId(), bound)
  }

  override def visitBoundedFrame(
      ctx: SqlBaseParser.BoundedFrameContext): BoundedFrame[Info, RawName] = {
    if (verbose) println(s"-------visitBoundedFrame called: ${ctx.getText}-------------")
    val bound = ctx.boundType.getType match {
      case SqlBaseLexer.PRECEDING => PRECEDING
      case SqlBaseLexer.FOLLOWING => FOLLOWING
    }
    val exp = visit(ctx.expression).asInstanceOf[RawExpression]
    BoundedFrame(nextId(), bound, exp)
  }

  override def visitCurrentRowBound(
      ctx: SqlBaseParser.CurrentRowBoundContext): CurrentRowBound[Info, RawName] = {
    if (verbose) println(s"-------visitCurrentRowBound called: ${ctx.getText}-------------")
    CurrentRowBound(nextId())
  }

  override def visitBasicStringLiteral(
      ctx: SqlBaseParser.BasicStringLiteralContext): StringLiteral[Info, RawName] = {
    if (verbose) println(s"-------visitStringLiteral called: ${ctx.getText}-------------")
    val text    = ctx.STRING.getText
    val literal = text.substring(1, text.size - 1).replaceAll("''", "'")
    StringLiteral(nextId(), literal)
  }

  override def visitBooleanValue(
      ctx: SqlBaseParser.BooleanValueContext): BooleanLiteral[Info, RawName] = {
    if (verbose) println(s"-------visitBooleanLiteral called: ${ctx.getText}-------------")
    BooleanLiteral(nextId(), ctx.getText.toBoolean)
  }

  override def visitDoubleLiteral(
      ctx: SqlBaseParser.DoubleLiteralContext): DoubleLiteral[Info, RawName] = {
    if (verbose) println(s"-------visitDoubleLiteral called: ${ctx.getText}-------------")
    DoubleLiteral(nextId(), ctx.getText.toDouble)
  }

  override def visitIntegerLiteral(
      ctx: SqlBaseParser.IntegerLiteralContext): IntLiteral[Info, RawName] = {
    if (verbose) println(s"-------visitIntegerLiteral called: ${ctx.getText}-------------")
    IntLiteral(nextId(), ctx.getText.toLong)
  }

  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext): Identifier = {
    if (verbose) println(s"-------visitUnquotedIdentifier called: ${ctx.getText}-------------")
    new Identifier(ctx.getText)
  }

  override def visitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Identifier = {
    val token      = ctx.getText
    val identifier = new Identifier(token.substring(1, token.size - 1))
    identifier
  }

}
