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

  def getColumnName(ctx: SqlBaseParser.QualifiedNameContext): RawName = {
    RawColumnName(ctx.identifier.asScala.map(_.getText).mkString("."))
  }

  def getColumnName(ctx: SqlBaseParser.IdentifierContext): RawName = {
    RawColumnName(ctx.getText)
  }

  def getTableName(ctx: SqlBaseParser.QualifiedNameContext): RawTableName = {
    RawTableName(ctx.identifier.asScala.map(_.getText).mkString("."))
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
    ???
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
    val sis = toUnsafeNEL(
      ctx.selectItem.asScala.map(visit(_).asInstanceOf[SelectItem[Info, RawName]]))
    val f = NonEmptyList.fromList(
      ctx.relation.asScala.map(visit(_).asInstanceOf[Relation[Info, RawName]]).toList)
    // TODO SetQuantifier
    QuerySpecification(nextId(), None, sis, f)
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
      ColumnAlias(nextId(), i.getText)
    })
    ColumnAliases(cols)
  }

  // TODO More relationPrimary
  override def visitTableName(
      ctx: SqlBaseParser.TableNameContext): RelationPrimary[Info, RawName] = {
    val ref: TableRef[Info, RawName] = TableRef(nextId(), getTableName(ctx.qualifiedName))
    TableName(nextId(), ref)
  }

  override def visitSubqueryExpression(
      ctx: SqlBaseParser.SubqueryExpressionContext): SubQueryExpr[Info, RawName] = {
    if (verbose) println(s"-------visitSubqueryExpression called: ${ctx.getText}-------------")
    SubQueryExpr(nextId(), visitQuery(ctx.query))
  }

  override def visitComparison(
      ctx: SqlBaseParser.ComparisonContext): ComparisonExpr[Info, RawName] = {
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

  override def visitNumericLiteral(
      ctx: SqlBaseParser.NumericLiteralContext): ConstantExpr[Info, RawName] = {
    if (verbose) println(s"-------visitNumericLiteral called: ${ctx.getText}-------------")
    // TODO this forces DoubleConstant
    ConstantExpr(nextId(), DoubleConstant(nextId(), ctx.getText.toDouble))
  }

}
