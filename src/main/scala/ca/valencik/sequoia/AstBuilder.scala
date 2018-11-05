package ca.valencik.bigsqlparse

import scala.collection.JavaConverters._

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  type QIdentifier         = Identifier[RawIdentifier]
  type QExpression         = Expression[RawIdentifier]
  type QRelation           = Relation[QualifiedName]
  type QQueryNoWith        = QueryNoWith[QualifiedName, RawIdentifier]
  type QQuerySpecification = QuerySpecification[QualifiedName, RawIdentifier]
  type QQuery              = Query[QualifiedName, RawIdentifier]
  type QWithQuery          = WithQuery[QualifiedName, RawIdentifier]
  type QWith               = With[QualifiedName, RawIdentifier]

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
        Some(JoinOn(visit(ctx.joinCriteria.booleanExpression).asInstanceOf[QExpression]))
      else if (jc.USING != null)
        Some(
          JoinUsing(
            ctx
              .joinCriteria()
              .identifier
              .asScala
              .map {
                case ic => Identifier(ic.getText)
              }
              .toList))
      else
        None
    }
  }

  def getRight(ctx: SqlBaseParser.JoinRelationContext): QRelation = {
    val rel =
      if (ctx.CROSS != null)
        visit(ctx.right)
      else if (ctx.NATURAL != null)
        visit(ctx.right)
      else
        visit(ctx.rightRelation)
    rel.asInstanceOf[QRelation]
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
    QualifiedName(ctx.identifier.asScala.map(_.getText).mkString("."))
  }

  def getFrom(ctx: SqlBaseParser.QuerySpecificationContext): From[QualifiedName] = {
    val relationOptions = ctx.relation.asScala.map { r =>
      Option(visit(r).asInstanceOf[QRelation])
    }.toList
    val relations = if (relationOptions.forall(_.isDefined)) Some(relationOptions.map(_.get)) else None
    From(relations)
  }

  override def visitWith(ctx: SqlBaseParser.WithContext): QWith = {
    val queries = ctx.namedQuery.asScala.map { n =>
      visitNamedQuery(n)
    }.toList
    With(ctx.RECURSIVE != null, queries)
  }

  override def visitNamedQuery(ctx: SqlBaseParser.NamedQueryContext): QWithQuery = {
    val name  = visit(ctx.name).asInstanceOf[QIdentifier]
    val query = visit(ctx.query).asInstanceOf[QQuery]
    // TODO fix column names
    WithQuery(name, query, None)
  }

  override def visitQuery(ctx: SqlBaseParser.QueryContext): QQuery = {
    val maybeWith =
      if (ctx.`with` != null)
        Option(visitWith(ctx.`with`).asInstanceOf[QWith])
      else
        None
    Query(maybeWith, visitQueryNoWith(ctx.queryNoWith))
  }

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext): QQueryNoWith = {
    val qso = visit(ctx.queryTerm).asInstanceOf[QQuerySpecification]
    val orderBy = {
      if (ctx.sortItem != null)
        Some(OrderBy(ctx.sortItem.asScala.map(visit(_).asInstanceOf[SortItem[RawIdentifier]]).toList))
      else
        None
    }
    val limit = if (ctx.LIMIT != null) Some(Limit(ctx.limit.getText)) else None
    QueryNoWith(qso, orderBy, limit)
  }

  override def visitSortItem(ctx: SqlBaseParser.SortItemContext): Node = {
    val exp: QExpression = visit(ctx.expression).asInstanceOf[QExpression]
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

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext): QQuerySpecification = {
    val select  = Select(ctx.selectItem.asScala.map(visit(_).asInstanceOf[SelectItem]).toList)
    val from    = getFrom(ctx)
    val where   = Where(if (ctx.where != null) Some(visit(ctx.where).asInstanceOf[QExpression]) else None)
    val groupBy = if (ctx.groupBy != null) visit(ctx.groupBy).asInstanceOf[GroupBy[RawIdentifier]] else GroupBy(List())
    val having  = Having(if (ctx.having != null) Some(visit(ctx.having).asInstanceOf[QExpression]) else None)
    QuerySpecification(select, from, where, groupBy, having)
  }

  override def visitSelectSingle(ctx: SqlBaseParser.SelectSingleContext): Node = {
    val alias = if (ctx.identifier != null) Option(visit(ctx.identifier).asInstanceOf[QIdentifier]) else None
    SingleColumn(visit(ctx.expression).asInstanceOf[QExpression], alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext): AllColumns = {
    if (ctx.qualifiedName != null) AllColumns(Some(getQualifiedName(ctx.qualifiedName))) else AllColumns(None)
  }

  override def visitGroupBy(ctx: SqlBaseParser.GroupByContext): Node = {
    val ges = ctx.groupingElement.asScala.map(visit(_).asInstanceOf[GroupingElement[RawIdentifier]]).toList
    GroupBy(ges)
  }

  override def visitSingleGroupingSet(ctx: SqlBaseParser.SingleGroupingSetContext): Node = {
    val ges = ctx.groupingSet.expression.asScala.map(visit(_).asInstanceOf[QIdentifier]).toList
    GroupingElement(ges)
  }

  override def visitJoinRelation(ctx: SqlBaseParser.JoinRelationContext): QRelation = {
    val left         = visit(ctx.left).asInstanceOf[QRelation]
    val right        = getRight(ctx)
    val joinType     = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    Join(joinType, left, right, joinCriteria)
  }

  override def visitSampledRelation(ctx: SqlBaseParser.SampledRelationContext): QRelation = {
    val child = visit(ctx.aliasedRelation).asInstanceOf[QRelation]
    if (ctx.TABLESAMPLE == null)
      child
    else {
      ???
    }
  }

  override def visitAliasedRelation(ctx: SqlBaseParser.AliasedRelationContext): QRelation = {
    val child = visit(ctx.relationPrimary).asInstanceOf[QRelation]
    if (ctx.identifier == null)
      child
    else {
      val alias = visit(ctx.identifier).asInstanceOf[QIdentifier]
      val columns: List[QIdentifier] =
        if (ctx.columnAliases != null)
          ctx.columnAliases.identifier.asScala.map(visit(_).asInstanceOf[QIdentifier]).toList
        else
          List.empty
      AliasedRelation(child, alias, columns)
    }
  }

  override def visitTableName(ctx: SqlBaseParser.TableNameContext): QRelation = {
    Table(getQualifiedName(ctx.qualifiedName))
  }

  override def visitPredicated(ctx: SqlBaseParser.PredicatedContext): Node = {
    if (ctx.predicate != null)
      visit(ctx.predicate)
    else
      visit(ctx.valueExpression)
  }

  override def visitNullPredicate(ctx: SqlBaseParser.NullPredicateContext): Node = {
    val exp = visit(ctx.value).asInstanceOf[QExpression]
    if (ctx.NOT != null)
      IsNotNullPredicate(exp)
    else
      IsNullPredicate(exp)
  }

  override def visitComparison(ctx: SqlBaseParser.ComparisonContext): Node = {
    val op = getComparisonOperator(ctx.comparisonOperator)
    ComparisonExpression(visit(ctx.value).asInstanceOf[QExpression], op, visit(ctx.right).asInstanceOf[QExpression])
  }

  override def visitLogicalBinary(ctx: SqlBaseParser.LogicalBinaryContext): Node = {
    val op = if (ctx.AND != null) AND else OR
    BooleanExpression(visit(ctx.left).asInstanceOf[QExpression], op, visit(ctx.right).asInstanceOf[QExpression])
  }

  override def visitValueExpressionDefault(ctx: SqlBaseParser.ValueExpressionDefaultContext): Node = {
    Identifier(ctx.getText)
  }

  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext): Node = {
    Identifier(ctx.getText)
  }

  override def visitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Node = {
    Identifier(ctx.getText)
  }

}
