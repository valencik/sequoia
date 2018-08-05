package ca.valencik.bigsqlparse

import scala.collection.JavaConversions._


class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

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

  def getJoinCriteria(ctx: SqlBaseParser.JoinRelationContext): Option[JoinCriteria] = {
    if (ctx.CROSS != null)
      None
    else if (ctx.NATURAL != null)
      Some(NaturalJoin)
    else {
      val jc = ctx.joinCriteria
      if (jc.ON != null)
        Some(JoinOn(visit(ctx.joinCriteria.booleanExpression).asInstanceOf[Expression]))
      else if (jc.USING != null)
        Some(JoinUsing(ctx.joinCriteria().identifier.map{
          case ic => Identifier(ic.getText)
        }.toList))
      else
        None
    }
  }

  def getRight(ctx: SqlBaseParser.JoinRelationContext): Relation = {
    val rel = if (ctx.CROSS != null)
      visit(ctx.right)
    else if (ctx.NATURAL != null)
      visit(ctx.right)
    else
      visit(ctx.rightRelation)
    rel.asInstanceOf[Relation]
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

  def getQualifiedName(ctx: SqlBaseParser.QualifiedNameContext): QualifiedName = {
    QualifiedName(ctx.identifier.map(_.getText).mkString("."))
  }

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext): QueryNoWith = {
    val qso = visit(ctx.queryTerm).asInstanceOf[QuerySpecification]
    val orderBy = {
      if (ctx.sortItem != null)
        Some(OrderBy(ctx.sortItem.map(visit(_).asInstanceOf[SortItem]).toList))
      else
        None
    }
    val limit = if (ctx.LIMIT != null) Some(Limit(ctx.limit.getText)) else None
    QueryNoWith(Some(qso), orderBy, limit)
  }

  override def visitSortItem(ctx: SqlBaseParser.SortItemContext): SortItem = {
    val exp: Expression = visit(ctx.expression).asInstanceOf[Expression]
    val ordering: Option[SortOrdering] = {
      if (ctx.ASC != null)
        Some(ASC)
      else if (ctx.DESC != null)
        Some(DESC)
      else
        None
    }
    val nullOrdering: Option[NullOrdering] = {
      if (ctx.NULLS != null && ctx.FIRST != null)
        Some(FIRST)
      else if (ctx.NULLS != null && ctx.LAST != null)
        Some(LAST)
      else
        None
    }
    SortItem(exp, ordering, nullOrdering)
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext): QuerySpecification = {
    val select = Select(ctx.selectItem.map(visit(_).asInstanceOf[SelectItem]).toList)
    val from = From(ctx.relation.map(visit(_).asInstanceOf[Relation]).toList)
    val where = Where(if (ctx.where != null) Some(visit(ctx.where).asInstanceOf[Expression]) else None)
    val groupBy = if (ctx.groupBy != null) visit(ctx.groupBy).asInstanceOf[GroupBy] else GroupBy(List())
    val having = Having(if (ctx.having != null) Some(visit(ctx.having).asInstanceOf[Expression]) else None)
    QuerySpecification(select, from, where, groupBy, having)
  }

  override def visitSelectSingle(ctx: SqlBaseParser.SelectSingleContext): SingleColumn = {
    val alias = if (ctx.identifier != null) Some(visit(ctx.identifier).asInstanceOf[Identifier]) else None
    SingleColumn(visit(ctx.expression()).asInstanceOf[Expression], alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext): AllColumns = {
    if (ctx.qualifiedName != null) AllColumns(Some(getQualifiedName(ctx.qualifiedName))) else AllColumns(None)
  }

  override def visitGroupBy(ctx: SqlBaseParser.GroupByContext): GroupBy = {
    val ges = ctx.groupingElement().map(visit(_).asInstanceOf[GroupingElement]).toList
    GroupBy(ges)
  }

  override def visitSingleGroupingSet(ctx: SqlBaseParser.SingleGroupingSetContext): GroupingElement = {
    val ges = ctx.groupingExpressions.expression().map(visit(_).asInstanceOf[Identifier]).toList
    GroupingElement(ges)
  }

  override def visitJoinRelation(ctx: SqlBaseParser.JoinRelationContext): Join = {
    val left = visit(ctx.left).asInstanceOf[Relation]
    val right = getRight(ctx)
    val joinType = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    Join(joinType, left, right, joinCriteria)
  }

  override def visitPredicated(ctx: SqlBaseParser.PredicatedContext): Node = {
    if (ctx.predicate != null)
      visit(ctx.predicate)
    else
      visit(ctx.valueExpression)
  }
  override def visitNullPredicate(ctx: SqlBaseParser.NullPredicateContext): Node = {
    val exp = visit(ctx.value).asInstanceOf[Expression]
    if (ctx.NOT != null)
      IsNotNullPredicate(exp)
    else
      IsNullPredicate(exp)
  }

  override def visitComparison(ctx: SqlBaseParser.ComparisonContext): ComparisonExpression = {
    val op = getComparisonOperator(ctx.comparisonOperator)
    ComparisonExpression(visit(ctx.value).asInstanceOf[Expression], op, visit(ctx.right).asInstanceOf[Expression])
  }
  override def visitLogicalBinary(ctx: SqlBaseParser.LogicalBinaryContext): BooleanExpression = {
    val op = if (ctx.AND != null) AND else OR
    BooleanExpression(visit(ctx.left).asInstanceOf[BooleanExpression], op, visit(ctx.right).asInstanceOf[BooleanExpression])
  }
  override def visitValueExpressionDefault(ctx: SqlBaseParser.ValueExpressionDefaultContext): Identifier = {
    Identifier(ctx.getText)
  }
  override def visitQualifiedName(ctx: SqlBaseParser.QualifiedNameContext): Name = {
    Name(ctx.getText)
  }
  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext): Identifier = {
    Identifier(ctx.getText)
  }
  override def visitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Identifier = {
    Identifier(ctx.getText)
  }

}
