package ca.valencik.sequoia

import scala.collection.JavaConverters._

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  type Info           = Int
  type RawQuery       = Query[Info, RawName]
  type RawQueryLimit  = QueryLimit[Info, RawName]
  type RawQuerySelect = QuerySelect[Info, RawName]
  type RawSelect      = Select[Info, RawName]
  type RawSelection   = Selection[Info, RawName]
  type RawTablish     = Tablish[Info, RawName]
  type RawExpression  = Expression[Info, RawName]

  val verbose: Boolean = false

  val nextId = {
    var i = 0;
    () =>
      { i += 1; i }
  }

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
    else if (ctx.NATURAL != null)
      Some(NaturalJoin(nextId()))
    else {
      val jc = ctx.joinCriteria
      if (jc.ON != null) {
        Some(JoinOn(nextId(), visit(jc.booleanExpression).asInstanceOf[RawExpression]))
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

  def getRight(ctx: SqlBaseParser.JoinRelationContext): RawTablish = {
    val rel =
      if (ctx.CROSS != null)
        visit(ctx.right)
      else if (ctx.NATURAL != null)
        visit(ctx.right)
      else
        visit(ctx.rightRelation)
    rel.asInstanceOf[RawTablish]
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

  // query:  with? queryNoWith ;
  // with: WITH RECURSIVE? namedQuery (',' namedQuery)* ;
  // queryNoWith: queryTerm...;
  // namedQuery: name=identifier (columnAliases)? AS '(' query ')' ;
  // columnAliases: '(' identifier (',' identifier)* ')' ;
  override def visitQuery(ctx: SqlBaseParser.QueryContext): RawQuery = {
    if (verbose) println(s"-------visitQuery called: ${ctx.getText}-------------")
    if (ctx.`with` != null) {
      val ctes: List[CTE[Info, RawName]] = ctx.`with`.namedQuery.asScala.map { n =>
        visitNamedQuery(n)
      }.toList
      QueryWith(nextId(), ctes, visitQueryNoWith(ctx.queryNoWith))
    } else
      QuerySelect(nextId(), visit(ctx.queryNoWith.queryTerm).asInstanceOf[RawSelect])
  }

  override def visitNamedQuery(ctx: SqlBaseParser.NamedQueryContext): CTE[Info, RawName] = {
    if (verbose) println(s"-------visitNamedQuery called: ${ctx.getText}-------------")
    val alias           = TablishAliasT(nextId(), ctx.name.getText)
    val query: RawQuery = visit(ctx.query).asInstanceOf[RawQuery]
    val cols: List[ColumnAlias[Info]] =
      if (ctx.columnAliases != null)
        ctx.columnAliases.identifier.asScala.map { a =>
          ColumnAlias(nextId(), a.getText)
        }.toList
      else List()
    CTE(nextId(), alias, cols, query)
  }

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext): RawQuery = {
    if (verbose) println(s"-------visitQueryNoWith called: ${ctx.getText}-------------")
    val qs: RawSelect = visit(ctx.queryTerm).asInstanceOf[RawSelect]
    if (ctx.LIMIT != null)
      QueryLimit(nextId(), Limit(nextId(), ctx.LIMIT.getText), qs)
    else
      QuerySelect(nextId(), qs)
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext): RawSelect = {
    if (verbose) println(s"-------visitQuerySpecification called: ${ctx.getText}-------------")
    val select =
      SelectCols(nextId(), ctx.selectItem.asScala.map(visit(_).asInstanceOf[RawSelection]).toList)
    val relationOptions = ctx.relation.asScala.map { r =>
      Option(visit(r).asInstanceOf[RawTablish])
    }.toList
    val from =
      if (relationOptions.size > 0 && relationOptions.forall(_.isDefined))
        Some(From(nextId(), relationOptions.map(_.get)))
      else None
    Select(nextId(), select, from)
  }

  override def visitSelectSingle(ctx: SqlBaseParser.SelectSingleContext): RawSelection = {
    val ident = if (ctx.identifier != null) Option(ctx.identifier.getText) else None
    val alias: Option[ColumnAlias[Info]] = ident.map { a =>
      ColumnAlias(nextId(), a)
    }
    val expr: RawExpression = visit(ctx.expression).asInstanceOf[RawExpression]
    SelectExpr(nextId(), expr, alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext): RawSelection = {
    val ref: Option[TableRef[Info, RawName]] =
      if (ctx.qualifiedName != null) Some(TableRef(nextId(), getTableName(ctx.qualifiedName)))
      else None
    SelectStar(nextId(), ref)
  }

  override def visitTableName(ctx: SqlBaseParser.TableNameContext): RawTablish = {
    val ref: TableRef[Info, RawName] = TableRef(nextId(), getTableName(ctx.qualifiedName))
    TablishTable(nextId(), TablishAliasNone[Info], ref)
  }

  override def visitJoinRelation(ctx: SqlBaseParser.JoinRelationContext): RawTablish = {
    val left         = visit(ctx.left).asInstanceOf[RawTablish]
    val right        = getRight(ctx)
    val joinType     = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    TablishJoin(nextId(), joinType, left, right, joinCriteria)
  }

  override def visitSubqueryExpression(
      ctx: SqlBaseParser.SubqueryExpressionContext): RawExpression = {
    if (verbose) println(s"-------visitSubqueryExpression called: ${ctx.getText}-------------")
    SubQueryExpr(nextId(), visit(ctx.query).asInstanceOf[RawQuery])
  }

  override def visitComparison(ctx: SqlBaseParser.ComparisonContext): Node = {
    val op    = getComparisonOperator(ctx.comparisonOperator)
    val left  = visit(ctx.value).asInstanceOf[RawExpression]
    val right = visit(ctx.right).asInstanceOf[RawExpression]
    ComparisonExpr(nextId(), left, op, right)
  }

  override def visitLogicalBinary(ctx: SqlBaseParser.LogicalBinaryContext): Node = {
    val op    = if (ctx.AND != null) AND else OR
    val left  = visit(ctx.left).asInstanceOf[RawExpression]
    val right = visit(ctx.right).asInstanceOf[RawExpression]
    BooleanExpr(nextId(), left, op, right)
  }

  override def visitColumnReference(ctx: SqlBaseParser.ColumnReferenceContext): RawExpression = {
    if (verbose) println(s"-------visitColumnReference called: ${ctx.getText}-------------")
    ColumnExpr(nextId(), ColumnRef(nextId(), getColumnName(ctx.identifier)))
  }

  override def visitNumericLiteral(ctx: SqlBaseParser.NumericLiteralContext): RawExpression = {
    if (verbose) println(s"-------visitNumericLiteral called: ${ctx.getText}-------------")
    // TODO this forces DoubleConstant
    ConstantExpr(nextId(), DoubleConstant(nextId(), ctx.getText.toDouble))
  }

}
