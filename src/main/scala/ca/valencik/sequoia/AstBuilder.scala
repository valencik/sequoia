package ca.valencik.sequoia

import scala.collection.JavaConverters._

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  type Info = Int
  type RawQuery = Query[RawNames, Info]
  type RawQueryLimit = QueryLimit[RawNames, Info]
  type RawQuerySelect = QuerySelect[RawNames, Info]
  type RawSelect = Select[RawNames, Info]
  type RawSelection = Selection[RawNames, Info]
  type RawTablish = Tablish[RawNames, Info]
  type RawExpression = Expression[RawNames, Info]

  val nextId = { var i = 0; () => { i += 1; i} }

  def getColumnName(ctx: SqlBaseParser.QualifiedNameContext): RawColumnName = {
    RawColumnName(ctx.identifier.asScala.map(_.getText).mkString("."))
  }

  def getColumnName(ctx: SqlBaseParser.IdentifierContext): RawColumnName = {
    RawColumnName(ctx.getText)
  }

  def getTableName(ctx: SqlBaseParser.QualifiedNameContext): RawTableName = {
    RawTableName(ctx.identifier.asScala.map(_.getText).mkString("."))
  }

  //override def visitQuery(ctx: SqlBaseParser.QueryContext): QQuery = {

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext): RawQuery = {
    val qs: RawSelect = visit(ctx.queryTerm).asInstanceOf[RawSelect]
    if (ctx.LIMIT != null)
      QueryLimit(nextId(), Limit(nextId(), ctx.LIMIT.getText), qs)
    else
      QuerySelect(nextId(), qs)
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext): RawSelect = {
    val select  = SelectCols(nextId(), ctx.selectItem.asScala.map(visit(_).asInstanceOf[RawSelection]).toSeq)
    val relationOptions = ctx.relation.asScala.map{ r=> Option(visit(r).asInstanceOf[RawTablish])}
    val from = if (relationOptions.size > 0 && relationOptions.forall(_.isDefined))
                 Some(From(nextId(), relationOptions.map(_.get)))
               else None
    Select(nextId(), select, from)
  }

  override def visitSelectSingle(ctx: SqlBaseParser.SelectSingleContext): RawSelection = {
    val ident = if (ctx.identifier != null) Option(ctx.identifier.getText) else None
    val alias: Option[ColumnAlias[Info]] = ident.map{a => ColumnAlias(nextId(), a)}
    val expr: RawExpression = visit(ctx.expression).asInstanceOf[RawExpression]
    SelectExpr(nextId(), expr, alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext): RawSelection = {
    val ref: Option[TableRef[RawNames, Info]] = if (ctx.qualifiedName != null) Some(TableRef(nextId(), getTableName(ctx.qualifiedName))) else None
    SelectStar(nextId(), ref)
  }

  override def visitTableName(ctx: SqlBaseParser.TableNameContext): RawTablish = {
    val ref: TableRef[RawNames, Info] = TableRef(nextId(), getTableName(ctx.qualifiedName))
    TablishTable(nextId(), TablishAliasNone[Info], ref)
  }

  override def visitColumnReference(ctx: SqlBaseParser.ColumnReferenceContext): RawExpression = {
    println(s"-------visitColumnReference called: ${ctx.getText}-------------")
    ColumnExpr(nextId(), ColumnRef(nextId(), getColumnName(ctx.identifier)))
  }

  override def visitNumericLiteral(ctx: SqlBaseParser.NumericLiteralContext): RawExpression = {
    println(s"-------visitNumericLiteral called: ${ctx.getText}-------------")
    ConstantExpr(nextId(), DoubleConstant(nextId(), ctx.getText.toDouble))
  }

}
