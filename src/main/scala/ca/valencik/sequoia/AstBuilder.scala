package ca.valencik.sequoia

import scala.collection.JavaConverters._

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  type Info           = Int
  type RawQuery       = Query[RawName, Info]
  type RawQueryLimit  = QueryLimit[RawName, Info]
  type RawQuerySelect = QuerySelect[RawName, Info]
  type RawSelect      = Select[RawName, Info]
  type RawSelection   = Selection[RawName, Info]
  type RawTablish     = Tablish[RawName, Info]
  type RawExpression  = Expression[RawName, Info]

  val verbose: Boolean = false

  val nextId = {
    var i = 0;
    () =>
      { i += 1; i }
  }

  def getColumnName(ctx: SqlBaseParser.QualifiedNameContext): RawColumnName = {
    RawColumnName(ctx.identifier.asScala.map(_.getText).mkString("."))
  }

  def getColumnName(ctx: SqlBaseParser.IdentifierContext): RawColumnName = {
    RawColumnName(ctx.getText)
  }

  def getTableName(ctx: SqlBaseParser.QualifiedNameContext): RawTableName = {
    RawTableName(ctx.identifier.asScala.map(_.getText).mkString("."))
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
      val ctes: List[CTE[RawName, Info]] = ctx.`with`.namedQuery.asScala.map { n =>
        visitNamedQuery(n)
      }.toList
      QueryWith(nextId(), ctes, visitQueryNoWith(ctx.queryNoWith))
    } else
      QuerySelect(nextId(), visit(ctx.queryNoWith.queryTerm).asInstanceOf[RawSelect])
  }

  override def visitNamedQuery(ctx: SqlBaseParser.NamedQueryContext): CTE[RawName, Info] = {
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
    val select = SelectCols(nextId(), ctx.selectItem.asScala.map(visit(_).asInstanceOf[RawSelection]).toList)
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
    val ref: Option[TableRef[RawName, Info]] =
      if (ctx.qualifiedName != null) Some(TableRef(nextId(), getTableName(ctx.qualifiedName))) else None
    SelectStar(nextId(), ref)
  }

  override def visitTableName(ctx: SqlBaseParser.TableNameContext): RawTablish = {
    val ref: TableRef[RawName, Info] = TableRef(nextId(), getTableName(ctx.qualifiedName))
    TablishTable(nextId(), TablishAliasNone[Info], ref)
  }

  override def visitSubqueryExpression(ctx: SqlBaseParser.SubqueryExpressionContext): RawExpression = {
    if (verbose) println(s"-------visitSubqueryExpression called: ${ctx.getText}-------------")
    SubQueryExpr(nextId(), visit(ctx.query).asInstanceOf[RawQuery])
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
