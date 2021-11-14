package ca.valencik.sequoia

import scala.jdk.CollectionConverters._

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  type Info                 = Int
  type RawExpression        = Expression[Info, RawName]
  type RawValueExpression   = ValueExpression[Info, RawName]
  type RawPrimaryExpression = PrimaryExpression[Info, RawName]

  val nextId = {
    var i = 0;
    () => { i += 1; i }
  }

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
      ctx: SqlBaseParser.JoinRelationContext
  ): Option[JoinCriteria[Info, RawName]] = {
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
        Some(JoinUsing(nextId(), ucs))
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

  def getQuantifier(ctx: SqlBaseParser.ComparisonQuantifierContext): Quantifier = {
    if (ctx.ALL != null)
      ALLQ
    else if (ctx.SOME != null)
      SOME
    else if (ctx.ANY != null)
      ANY
    else
      ???
  }

  // -- Overrides
  override def visitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): Node = {
    visit(ctx.statement)
  }

  override def visitQuery(ctx: SqlBaseParser.QueryContext): Query[Info, RawName] = {
    val maybeWith = if (ctx.`with` != null) Some(visitWith(ctx.`with`)) else None
    Query(nextId(), maybeWith, visitQueryNoWith(ctx.queryNoWith))
  }

  override def visitWith(ctx: SqlBaseParser.WithContext): With[Info, RawName] = {
    val nqs = ctx.namedQuery.asScala.map(visitNamedQuery).toList
    With(nextId(), nqs)
  }

  override def visitQueryNoWith(
      ctx: SqlBaseParser.QueryNoWithContext
  ): QueryNoWith[Info, RawName] = {
    val qt = visit(ctx.queryTerm).asInstanceOf[QueryTerm[Info, RawName]]
    val orderBy = {
      if (ctx.sortItem.size > 0)
        Some(OrderBy(nextId(), ctx.sortItem.asScala.map(visitSortItem).toList))
      else
        None
    }
    val limit = if (ctx.LIMIT != null) Some(Limit(nextId(), ctx.limit.getText)) else None
    QueryNoWith(nextId(), qt, orderBy, limit)
  }

  override def visitSetOperation(
      ctx: SqlBaseParser.SetOperationContext
  ): SetOperation[Info, RawName] = {
    val left  = visit(ctx.left).asInstanceOf[QueryTerm[Info, RawName]]
    val right = visit(ctx.right).asInstanceOf[QueryTerm[Info, RawName]]
    val sq    = maybeSetQuantifier(ctx.setQuantifier)
    val op = ctx.operator.getType match {
      case SqlBaseLexer.INTERSECT => INTERSECT
      case SqlBaseLexer.UNION     => UNION
      case SqlBaseLexer.EXCEPT    => EXCEPT
      case _                      => throw new Exception("Impossible parse error")
    }
    SetOperation(nextId(), left, op, sq, right)
  }

  override def visitSortItem(ctx: SqlBaseParser.SortItemContext): SortItem[Info, RawName] = {
    val exp = visitExpression(ctx.expression).asInstanceOf[RawExpression]
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
      ctx: SqlBaseParser.QuerySpecificationContext
  ): QuerySpecification[Info, RawName] = {
    val sq  = maybeSetQuantifier(ctx.setQuantifier)
    val sis = ctx.selectItem.asScala.map(visit(_).asInstanceOf[SelectItem[Info, RawName]]).toList
    val f   = ctx.relation.asScala.map(visit(_).asInstanceOf[Relation[Info, RawName]]).toList
    val w   = if (ctx.where != null) Some(visit(ctx.where).asInstanceOf[RawExpression]) else None
    val g =
      if (ctx.groupBy != null) Some(visit(ctx.groupBy).asInstanceOf[GroupBy[Info, RawName]])
      else None
    val h = if (ctx.having != null) Some(visit(ctx.having).asInstanceOf[RawExpression]) else None

    QuerySpecification(nextId(), sq, sis, f, w, g, h)
  }

  override def visitInlineTable(
      ctx: SqlBaseParser.InlineTableContext
  ): InlineTable[Info, RawName] = {
    val vs = ctx.expression.asScala.map { visit(_).asInstanceOf[RawExpression] }.toList
    InlineTable(nextId(), vs)
  }

  override def visitSubquery(ctx: SqlBaseParser.SubqueryContext): SubQuery[Info, RawName] = {
    SubQuery(nextId(), visitQueryNoWith(ctx.queryNoWith))
  }

  override def visitGroupBy(ctx: SqlBaseParser.GroupByContext): GroupBy[Info, RawName] = {
    val sq = maybeSetQuantifier(ctx.setQuantifier)
    val ges = ctx.groupingElement.asScala.map { g =>
      visit(g).asInstanceOf[GroupingElement[Info, RawName]]
    }.toList
    GroupBy(nextId(), sq, ges)
  }

  override def visitSingleGroupingSet(
      ctx: SqlBaseParser.SingleGroupingSetContext
  ): GroupingElement[Info, RawName] = {
    SingleGroupingSet(nextId(), visit(ctx.groupingSet).asInstanceOf[GroupingSet[Info, RawName]])
  }

  override def visitGroupingSet(
      ctx: SqlBaseParser.GroupingSetContext
  ): GroupingSet[Info, RawName] = {
    val es = ctx.expression.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    GroupingSet(nextId(), es)
  }

  override def visitNamedQuery(ctx: SqlBaseParser.NamedQueryContext): NamedQuery[Info, RawName] = {
    val query = visitQuery(ctx.query)
    val colAs =
      if (ctx.columnAliases != null)
        Some(visitColumnAliases(ctx.columnAliases))
      else
        None
    NamedQuery(nextId(), ctx.name.getText, colAs, query)
  }

  override def visitSelectSingle(
      ctx: SqlBaseParser.SelectSingleContext
  ): SelectSingle[Info, RawName] = {
    val ident = if (ctx.identifier != null) Some(ctx.identifier.getText) else None
    val alias: Option[ColumnAlias[Info]] = ident.map { a =>
      ColumnAlias(nextId(), a)
    }
    val expr = visit(ctx.expression).asInstanceOf[RawExpression]
    SelectSingle(nextId(), expr, alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext): SelectAll[Info, RawName] = {
    val colAs = Option(ctx.columnAliases()).map(visitColumnAliases)
    val target: Option[TableRef[Info, RawName]] = Option(ctx.primaryExpression())
      .map(visit(_).asInstanceOf[ColumnExpr[Info, RawName]])
      .map(ce => TableRef(nextId(), RawTableName(ce.col.value.value)))
    SelectAll(nextId(), target, colAs)
  }

  override def visitJoinRelation(
      ctx: SqlBaseParser.JoinRelationContext
  ): JoinRelation[Info, RawName] = {
    val left         = visit(ctx.left).asInstanceOf[Relation[Info, RawName]]
    val right        = getRight(ctx)
    val joinType     = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    JoinRelation(nextId(), joinType, left, right, joinCriteria)
  }

  override def visitSampledRelation(
      ctx: SqlBaseParser.SampledRelationContext
  ): SampledRelation[Info, RawName] = {
    val ar = visitAliasedRelation(ctx.aliasedRelation)
    val ts = if (ctx.sampleType != null) {
      val st = if (ctx.sampleType.BERNOULLI != null) BERNOULLI else SYSTEM
      val p  = visit(ctx.percentage).asInstanceOf[RawExpression]
      Some(TableSample(nextId(), st, p))
    } else None
    SampledRelation(nextId(), ar, ts)
  }

  override def visitAliasedRelation(
      ctx: SqlBaseParser.AliasedRelationContext
  ): AliasedRelation[Info, RawName] = {
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
    val cols = ctx.identifier.asScala.map { i =>
      ColumnAlias(nextId(), visit(i).asInstanceOf[Identifier].value)
    }.toList
    ColumnAliases(cols)
  }

  override def visitTableName(
      ctx: SqlBaseParser.TableNameContext
  ): RelationPrimary[Info, RawName] = {
    val ref: TableRef[Info, RawName] = TableRef(nextId(), getTableName(ctx.qualifiedName))
    TableName(nextId(), ref)
  }

  override def visitSubqueryRelation(
      ctx: SqlBaseParser.SubqueryRelationContext
  ): RelationPrimary[Info, RawName] = {
    SubQueryRelation(nextId(), visit(ctx.query).asInstanceOf[Query[Info, RawName]])
  }

  override def visitUnnest(ctx: SqlBaseParser.UnnestContext): RelationPrimary[Info, RawName] = {
    val exps    = ctx.expression.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    val ordinal = ctx.ORDINALITY != null
    Unnest(nextId(), exps, ordinal)
  }

  override def visitLateral(ctx: SqlBaseParser.LateralContext): RelationPrimary[Info, RawName] = {
    val q = visitQuery(ctx.query)
    LateralRelation(nextId(), q)
  }

  override def visitParenthesizedRelation(
      ctx: SqlBaseParser.ParenthesizedRelationContext
  ): RelationPrimary[Info, RawName] = {
    val r = visit(ctx.relation).asInstanceOf[Relation[Info, RawName]]
    ParenthesizedRelation(nextId(), r)
  }

  override def visitArithmeticUnary(
      ctx: SqlBaseParser.ArithmeticUnaryContext
  ): ArithmeticUnary[Info, RawName] = {
    val op: Sign = ctx.operator.getType match {
      case SqlBaseLexer.PLUS  => PLUS
      case SqlBaseLexer.MINUS => MINUS
      case _                  => throw new Exception("Impossible parse error")
    }
    val value = visit(ctx.valueExpression).asInstanceOf[RawValueExpression]
    ArithmeticUnary(nextId(), op, value)
  }

  override def visitArithmeticBinary(
      ctx: SqlBaseParser.ArithmeticBinaryContext
  ): ArithmeticBinary[Info, RawName] = {
    val left  = visit(ctx.left).asInstanceOf[ValueExpression[Info, RawName]]
    val right = visit(ctx.right).asInstanceOf[ValueExpression[Info, RawName]]
    val op = ctx.operator.getType match {
      case SqlBaseLexer.PLUS     => ADD
      case SqlBaseLexer.MINUS    => SUBTRACT
      case SqlBaseLexer.ASTERISK => MULTIPLY
      case SqlBaseLexer.SLASH    => DIVIDE
      case SqlBaseLexer.PERCENT  => MODULUS
      case _                     => throw new Exception("Impossible parse error")
    }
    ArithmeticBinary(nextId(), left, op, right)
  }

  override def visitConcatenation(
      ctx: SqlBaseParser.ConcatenationContext
  ): FunctionCall[Info, RawName] = {
    val left  = visit(ctx.left).asInstanceOf[RawExpression]
    val right = visit(ctx.right).asInstanceOf[RawExpression]
    FunctionCall(nextId(), "concat", None, List(left, right), None, None, None)
  }

  override def visitPredicated(ctx: SqlBaseParser.PredicatedContext): Node = {
    if (ctx.predicate != null) visit(ctx.predicate) else visit(ctx.valueExpression)
  }

  override def visitComparison(
      ctx: SqlBaseParser.ComparisonContext
  ): ComparisonExpr[Info, RawName] = {
    val op    = getComparisonOperator(ctx.comparisonOperator)
    val left  = visit(ctx.value).asInstanceOf[RawValueExpression]
    val right = visit(ctx.right).asInstanceOf[RawValueExpression]
    ComparisonExpr(nextId(), left, op, right)
  }

  override def visitQuantifiedComparison(
      ctx: SqlBaseParser.QuantifiedComparisonContext
  ): QuantifiedComparison[Info, RawName] = {
    val op    = getComparisonOperator(ctx.comparisonOperator)
    val quant = getQuantifier(ctx.comparisonQuantifier)
    val value = visit(ctx.value).asInstanceOf[RawValueExpression]
    val query = visitQuery(ctx.query)
    QuantifiedComparison(nextId(), value, op, quant, query)
  }

  override def visitBetween(ctx: SqlBaseParser.BetweenContext): Predicate[Info, RawName] = {
    val value = visit(ctx.value).asInstanceOf[RawValueExpression]
    val lower = visit(ctx.lower).asInstanceOf[RawValueExpression]
    val upper = visit(ctx.upper).asInstanceOf[RawValueExpression]
    val res   = Between(nextId(), value, lower, upper)
    if (ctx.NOT != null) NotPredicate(nextId(), res) else res
  }

  override def visitInList(ctx: SqlBaseParser.InListContext): Predicate[Info, RawName] = {
    val value = visit(ctx.value).asInstanceOf[RawValueExpression]
    val exps  = ctx.expression.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    val res   = InList(nextId(), value, exps)
    if (ctx.NOT != null) NotPredicate(nextId(), res) else res
  }

  override def visitInSubquery(ctx: SqlBaseParser.InSubqueryContext): Predicate[Info, RawName] = {
    val value = visit(ctx.value).asInstanceOf[RawValueExpression]
    val query = visitQuery(ctx.query)
    val res   = InSubQuery(nextId(), value, query)
    if (ctx.NOT != null) NotPredicate(nextId(), res) else res
  }

  override def visitLike(ctx: SqlBaseParser.LikeContext): Predicate[Info, RawName] = {
    val value   = visit(ctx.value).asInstanceOf[RawValueExpression]
    val pattern = visit(ctx.pattern).asInstanceOf[RawValueExpression]
    val escape =
      if (ctx.escape != null) Some(visit(ctx.escape).asInstanceOf[RawValueExpression]) else None
    val res = Like(nextId(), value, pattern, escape)
    if (ctx.NOT != null) NotPredicate(nextId(), res) else res
  }

  override def visitNullPredicate(
      ctx: SqlBaseParser.NullPredicateContext
  ): Predicate[Info, RawName] = {
    val expr = visit(ctx.value).asInstanceOf[RawValueExpression]
    val res  = NullPredicate(nextId(), expr)
    if (ctx.NOT != null) NotPredicate(nextId(), res) else res
  }

  override def visitDistinctFrom(
      ctx: SqlBaseParser.DistinctFromContext
  ): Predicate[Info, RawName] = {
    val value = visit(ctx.value).asInstanceOf[RawValueExpression]
    val right = visit(ctx.right).asInstanceOf[RawValueExpression]
    val res   = DistinctFrom(nextId(), value, right)
    if (ctx.NOT != null) NotPredicate(nextId(), res) else res
  }

  override def visitSubscript(
      ctx: SqlBaseParser.SubscriptContext
  ): PrimaryExpression[Info, RawName] = {
    val value = visit(ctx.value).asInstanceOf[RawPrimaryExpression]
    val index = visit(ctx.index).asInstanceOf[RawValueExpression]
    Subscript(nextId(), value, index)
  }

  override def visitDereference(
      ctx: SqlBaseParser.DereferenceContext
  ): PrimaryExpression[Info, RawName] = {
    val base      = visit(ctx.base).asInstanceOf[RawPrimaryExpression]
    val fieldName = visit(ctx.fieldName).asInstanceOf[Identifier]
    DereferenceExpr(nextId(), base, fieldName.value)
  }

  override def visitExtract(ctx: SqlBaseParser.ExtractContext): Extract[Info, RawName] = {
    val field = ctx.identifier.getText
    val exp   = visit(ctx.valueExpression).asInstanceOf[RawValueExpression]
    Extract(nextId(), field, exp)
  }

  override def visitParenthesizedExpression(
      ctx: SqlBaseParser.ParenthesizedExpressionContext
  ): Expression[Info, RawName] = {
    visit(ctx.expression).asInstanceOf[RawExpression]
  }

  override def visitSubqueryExpression(
      ctx: SqlBaseParser.SubqueryExpressionContext
  ): SubQueryExpr[Info, RawName] = {
    SubQueryExpr(nextId(), visitQuery(ctx.query))
  }

  override def visitExists(ctx: SqlBaseParser.ExistsContext): ExistsExpr[Info, RawName] = {
    ExistsExpr(nextId(), visitQuery(ctx.query))
  }

  override def visitSimpleCase(ctx: SqlBaseParser.SimpleCaseContext): SimpleCase[Info, RawName] = {
    val exp         = visit(ctx.operand).asInstanceOf[RawExpression]
    val whenClauses = ctx.whenClause.asScala.map(visitWhenClause).toList
    val elseExpression =
      if (ctx.elseExpression != null) Some(visit(ctx.elseExpression).asInstanceOf[RawExpression])
      else None
    SimpleCase(nextId(), exp, whenClauses, elseExpression)
  }

  override def visitSearchedCase(
      ctx: SqlBaseParser.SearchedCaseContext
  ): SearchedCase[Info, RawName] = {
    val whenClauses = ctx.whenClause.asScala.map(visitWhenClause).toList
    val elseExpression =
      if (ctx.elseExpression != null) Some(visit(ctx.elseExpression).asInstanceOf[RawExpression])
      else None
    SearchedCase(nextId(), whenClauses, elseExpression)
  }

  override def visitCast(ctx: SqlBaseParser.CastContext): Cast[Info, RawName] = {
    val exp = visit(ctx.expression).asInstanceOf[RawExpression]
    // TODO We could handle this better
    val `type`: String = ctx.`type`.getText
    val isTry          = ctx.TRY_CAST != null
    Cast(nextId(), exp, `type`, isTry)
  }

  override def visitWhenClause(ctx: SqlBaseParser.WhenClauseContext): WhenClause[Info, RawName] = {
    val cond = visit(ctx.condition).asInstanceOf[RawExpression]
    val res  = visit(ctx.result).asInstanceOf[RawExpression]
    WhenClause(nextId(), cond, res)
  }

  override def visitLogicalBinary(
      ctx: SqlBaseParser.LogicalBinaryContext
  ): LogicalBinary[Info, RawName] = {
    val op    = if (ctx.AND != null) AND else OR
    val left  = visit(ctx.left).asInstanceOf[RawExpression]
    val right = visit(ctx.right).asInstanceOf[RawExpression]
    LogicalBinary(nextId(), left, op, right)
  }

  override def visitColumnReference(
      ctx: SqlBaseParser.ColumnReferenceContext
  ): ColumnExpr[Info, RawName] = {
    ColumnExpr(nextId(), ColumnRef(nextId(), getColumnName(ctx.identifier)))
  }

  override def visitPosition(ctx: SqlBaseParser.PositionContext): FunctionCall[Info, RawName] = {
    val args = ctx.valueExpression.asScala.map(visit(_).asInstanceOf[RawValueExpression]).toList
    FunctionCall(nextId(), "strpos", None, args, None, None, None)
  }

  override def visitRowConstructor(ctx: SqlBaseParser.RowConstructorContext): Row[Info, RawName] = {
    val exps = ctx.expression.asScala.map { visit(_).asInstanceOf[RawExpression] }.toList
    Row(nextId(), exps)
  }

  override def visitFunctionCall(
      ctx: SqlBaseParser.FunctionCallContext
  ): FunctionCall[Info, RawName] = {
    val name  = qualifiedNameAsString(ctx.qualifiedName)
    val sq    = maybeSetQuantifier(ctx.setQuantifier)
    val exprs = ctx.expression.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    val orderby = {
      if (ctx.sortItem.size > 0)
        Some(OrderBy(nextId(), ctx.sortItem.asScala.map(visitSortItem).toList))
      else
        None
    }
    val filter = if (ctx.filter != null) Some(visitFilter(ctx.filter)) else None
    val over   = if (ctx.over != null) Some(visitOver(ctx.over)) else None

    FunctionCall(nextId(), name, sq, exprs, orderby, filter, over)
  }

  override def visitFilter(ctx: SqlBaseParser.FilterContext): FunctionFilter[Info, RawName] = {
    val exp = visit(ctx.booleanExpression).asInstanceOf[Expression[Info, RawName]]
    FunctionFilter(nextId(), exp)
  }

  override def visitWindowDefinition(
      ctx: SqlBaseParser.WindowDefinitionContext
  ): WindowDefinition[Info, RawName] = {
    val spec = visitWindowSpecification(ctx.windowSpecification)
    WindowDefinition(nextId(), ctx.name.getText(), spec)
  }

  override def visitWindowSpecification(
      ctx: SqlBaseParser.WindowSpecificationContext
  ): WindowSpecification[Info, RawName] = {
    val partitionBy = ctx.partition.asScala.map(visit(_).asInstanceOf[RawExpression]).toList
    val orderBy = {
      if (ctx.sortItem.size > 0)
        Some(OrderBy(nextId(), ctx.sortItem.asScala.map(visitSortItem).toList))
      else
        None
    }
    val window = if (ctx.windowFrame != null) Some(visitWindowFrame(ctx.windowFrame)) else None
    WindowSpecification(nextId(), partitionBy, orderBy, window)
  }

  override def visitOver(ctx: SqlBaseParser.OverContext): FunctionOver[Info, RawName] = {
    if (ctx.windowName != null)
      WindowReference(nextId(), Identifier(ctx.windowName.getText()))
    else
      visitWindowSpecification(ctx.windowSpecification())
  }

  override def visitWindowFrame(
      ctx: SqlBaseParser.WindowFrameContext
  ): WindowFrame[Info, RawName] = {
    val ft = ctx.frameType.getType match {
      case SqlBaseLexer.RANGE => RANGE
      case SqlBaseLexer.ROWS  => ROWS
      case _                  => throw new Exception("Impossible parse error")
    }
    val start = visit(ctx.start).asInstanceOf[FrameBound[Info, RawName]]
    val end =
      if (ctx.end != null) Some(visit(ctx.end).asInstanceOf[FrameBound[Info, RawName]]) else None
    WindowFrame(nextId(), ft, start, end)
  }

  override def visitUnboundedFrame(
      ctx: SqlBaseParser.UnboundedFrameContext
  ): UnboundedFrame[Info, RawName] = {
    val bound = ctx.boundType.getType match {
      case SqlBaseLexer.PRECEDING => PRECEDING
      case SqlBaseLexer.FOLLOWING => FOLLOWING
      case _                      => throw new Exception("Impossible parse error")
    }
    UnboundedFrame(nextId(), bound)
  }

  override def visitBoundedFrame(
      ctx: SqlBaseParser.BoundedFrameContext
  ): BoundedFrame[Info, RawName] = {
    val bound = ctx.boundType.getType match {
      case SqlBaseLexer.PRECEDING => PRECEDING
      case SqlBaseLexer.FOLLOWING => FOLLOWING
      case _                      => throw new Exception("Impossible parse error")
    }
    val exp = visit(ctx.expression).asInstanceOf[RawExpression]
    BoundedFrame(nextId(), bound, exp)
  }

  override def visitCurrentRowBound(
      ctx: SqlBaseParser.CurrentRowBoundContext
  ): CurrentRowBound[Info, RawName] = {
    CurrentRowBound(nextId())
  }

  override def visitIntervalField(ctx: SqlBaseParser.IntervalFieldContext): IntervalField = {
    if (ctx.YEAR != null)
      YEAR
    else if (ctx.MONTH != null)
      MONTH
    else if (ctx.DAY != null)
      DAY
    else if (ctx.HOUR != null)
      HOUR
    else if (ctx.MINUTE != null)
      MINUTE
    else if (ctx.SECOND != null)
      SECOND
    else
      ???
  }

  override def visitInterval(ctx: SqlBaseParser.IntervalContext): IntervalLiteral[Info, RawName] = {
    val sign =
      if (ctx.sign != null)
        ctx.sign.getType match {
          case SqlBaseLexer.PLUS  => PLUS
          case SqlBaseLexer.MINUS => MINUS
          case _                  => throw new Exception("Impossible parse error")
        }
      else PLUS
    val value = visit(ctx.string).asInstanceOf[StringLiteral[Info, RawName]]
    val from  = visitIntervalField(ctx.from)
    val to    = if (ctx.to != null) Some(visitIntervalField(ctx.to)) else None
    IntervalLiteral(nextId(), sign, value, from, to)
  }

  override def visitSpecialDateTimeFunction(
      ctx: SqlBaseParser.SpecialDateTimeFunctionContext
  ): SpecialDateTimeFunc[Info, RawName] = {
    val name = ctx.name.getType match {
      case SqlBaseLexer.CURRENT_DATE      => CURRENT_DATE
      case SqlBaseLexer.CURRENT_TIME      => CURRENT_TIME
      case SqlBaseLexer.CURRENT_TIMESTAMP => CURRENT_TIMESTAMP
      case SqlBaseLexer.LOCALTIME         => LOCALTIME
      case SqlBaseLexer.LOCALTIMESTAMP    => LOCALTIMESTAMP
      case _                              => throw new Exception("Impossible parse error")
    }
    val precision = if (ctx.precision != null) Some(ctx.precision.getText.toInt) else None
    SpecialDateTimeFunc(nextId(), name, precision)
  }

  override def visitBasicStringLiteral(
      ctx: SqlBaseParser.BasicStringLiteralContext
  ): StringLiteral[Info, RawName] = {
    val text    = ctx.STRING.getText
    val literal = text.substring(1, text.size - 1).replaceAll("''", "'")
    StringLiteral(nextId(), literal)
  }

  override def visitBooleanValue(
      ctx: SqlBaseParser.BooleanValueContext
  ): BooleanLiteral[Info, RawName] = {
    BooleanLiteral(nextId(), ctx.getText.toBoolean)
  }

  override def visitNullLiteral(
      ctx: SqlBaseParser.NullLiteralContext
  ): NullLiteral[Info, RawName] = {
    NullLiteral(nextId())
  }

  override def visitDecimalLiteral(
      ctx: SqlBaseParser.DecimalLiteralContext
  ): DecimalLiteral[Info, RawName] = {
    DecimalLiteral(nextId(), ctx.getText.toDouble)
  }

  override def visitDoubleLiteral(
      ctx: SqlBaseParser.DoubleLiteralContext
  ): DoubleLiteral[Info, RawName] = {
    DoubleLiteral(nextId(), ctx.getText.toDouble)
  }

  override def visitIntegerLiteral(
      ctx: SqlBaseParser.IntegerLiteralContext
  ): IntLiteral[Info, RawName] = {
    IntLiteral(nextId(), ctx.getText.toLong)
  }

  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext): Identifier = {
    new Identifier(ctx.getText)
  }

  override def visitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Identifier = {
    val token      = ctx.getText
    val identifier = new Identifier(token.substring(1, token.size - 1))
    identifier
  }

}
