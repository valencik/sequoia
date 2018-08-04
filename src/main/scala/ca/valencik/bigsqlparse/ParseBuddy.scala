package ca.valencik.bigsqlparse

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.TerminalNode
import scala.util.{ Failure, Success, Try }

import scala.collection.JavaConversions._


sealed trait Node
case class Name(text: String) extends Node with Relation
case class Select(selectItems: List[SelectItem]) extends Node

sealed trait SelectItem extends Node
case class SingleColumn(expression: Expression, alias: Option[Identifier]) extends SelectItem
case class AllColumns(name: Option[QualifiedName]) extends SelectItem

sealed trait Relation
case class From(relations: List[Relation]) extends Node

sealed trait Expression
case class Identifier(name: String) extends Node with Expression
case class QualifiedName(name: String) extends Node with Expression
case class BooleanExpression(left: Expression, op: Operator, right: Expression) extends Node with Expression
case class ComparisonExpression(left: Expression, op: Comparison, right: Expression) extends Node with Expression
case class IsNullPredicate(value: Expression) extends Node with Expression
case class IsNotNullPredicate(value: Expression) extends Node with Expression

sealed trait Operator
case object AND extends Operator
case object OR extends Operator

sealed trait Comparison
case object EQ extends Comparison
case object NEQ extends Comparison
case object LT extends Comparison
case object LTE extends Comparison
case object GT extends Comparison
case object GTE extends Comparison

case class SortItem(expression: Expression, ordering: Option[SortOrdering], nullOrdering: Option[NullOrdering]) extends Node

sealed trait SortOrdering
case object ASC extends SortOrdering
case object DESC extends SortOrdering

sealed trait NullOrdering
case object FIRST extends NullOrdering
case object LAST extends NullOrdering

case class Where(expression: Option[Expression]) extends Node
case class GroupingElement(groupingSet: List[Expression]) extends Node
case class GroupBy(groupingElements: List[GroupingElement]) extends Node
case class Having(expression: Option[Expression]) extends Node
case class QuerySpecification(
  select: Select,
  from: From,
  where: Where,
  groupBy: GroupBy,
  having: Having
) extends Node


case class OrderBy(items: List[SortItem]) extends Node
case class Limit(value: String) extends Node
case class QueryNoWith(querySpecification: Option[QuerySpecification], orderBy: Option[OrderBy], limit: Option[Limit]) extends Node

sealed trait JoinType
case object LeftJoin extends JoinType
case object RightJoin extends JoinType
case object FullJoin extends JoinType
case object InnerJoin extends JoinType
case object CrossJoin extends JoinType
sealed trait JoinCriteria
case object NaturalJoin extends JoinCriteria
case class JoinOn(expression: Expression) extends JoinCriteria
case class JoinUsing(columns: List[Identifier]) extends JoinCriteria
case class Join(jointype: JoinType, left: Relation, right: Relation, criterea: Option[JoinCriteria]) extends Node with Relation
case class SampledRelation(text: String) extends Node with Relation

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

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext) = {
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

  override def visitSortItem(ctx: SqlBaseParser.SortItemContext) = {
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

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext) = {
    val select = Select(ctx.selectItem.map(visit(_).asInstanceOf[SelectItem]).toList)
    val from = From(ctx.relation.map(visit(_).asInstanceOf[Relation]).toList)
    val where = Where(if (ctx.where != null) Some(visit(ctx.where).asInstanceOf[Expression]) else None)
    val groupBy = if (ctx.groupBy != null) visit(ctx.groupBy).asInstanceOf[GroupBy] else GroupBy(List())
    val having = Having(if (ctx.having != null) Some(visit(ctx.having).asInstanceOf[Expression]) else None)
    QuerySpecification(select, from, where, groupBy, having)
  }

  override def visitSelectSingle(ctx: SqlBaseParser.SelectSingleContext) = {
    val alias = if (ctx.identifier != null) Some(visit(ctx.identifier).asInstanceOf[Identifier]) else None
    SingleColumn(visit(ctx.expression()).asInstanceOf[Expression], alias)
  }

  override def visitSelectAll(ctx: SqlBaseParser.SelectAllContext) = {
    if (ctx.qualifiedName != null) AllColumns(Some(getQualifiedName(ctx.qualifiedName))) else AllColumns(None)
  }

  override def visitGroupBy(ctx: SqlBaseParser.GroupByContext) = {
    val ges = ctx.groupingElement().map(visit(_).asInstanceOf[GroupingElement]).toList
    GroupBy(ges)
  }

  override def visitSingleGroupingSet(ctx: SqlBaseParser.SingleGroupingSetContext) = {
    val ges = ctx.groupingExpressions.expression().map(visit(_).asInstanceOf[Identifier]).toList
    GroupingElement(ges)
  }

  override def visitJoinRelation(ctx: SqlBaseParser.JoinRelationContext) = {
    val left = visit(ctx.left).asInstanceOf[Relation]
    val right = getRight(ctx)
    val joinType = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    Join(joinType, left, right, joinCriteria)
  }

  override def visitPredicated(ctx: SqlBaseParser.PredicatedContext) = {
    if (ctx.predicate != null)
      visit(ctx.predicate)
    else
      visit(ctx.valueExpression)
  }
  override def visitNullPredicate(ctx: SqlBaseParser.NullPredicateContext) = {
    val exp = visit(ctx.value).asInstanceOf[Expression]
    if (ctx.NOT != null)
      IsNotNullPredicate(exp)
    else
      IsNullPredicate(exp)
  }

  override def visitComparison(ctx: SqlBaseParser.ComparisonContext) = {
    val op = getComparisonOperator(ctx.comparisonOperator)
    ComparisonExpression(visit(ctx.value).asInstanceOf[Expression], op, visit(ctx.right).asInstanceOf[Expression])
  }
  override def visitLogicalBinary(ctx: SqlBaseParser.LogicalBinaryContext) = {
    val op = if (ctx.AND != null) AND else OR
    BooleanExpression(visit(ctx.left).asInstanceOf[BooleanExpression], op, visit(ctx.right).asInstanceOf[BooleanExpression])
  }
  override def visitValueExpressionDefault(ctx: SqlBaseParser.ValueExpressionDefaultContext) = {
    Identifier(ctx.getText)
  }
  override def visitQualifiedName(ctx: SqlBaseParser.QualifiedNameContext) = {
    Name(ctx.getText)
  }
  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext) = {
    Identifier(ctx.getText)
  }
  override def visitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext) = {
    Identifier(ctx.getText)
  }
  override def visitBackQuotedIdentifier(ctx: SqlBaseParser.BackQuotedIdentifierContext) = {
    Identifier(ctx.getText)
  }
  override def visitDigitIdentifier(ctx: SqlBaseParser.DigitIdentifierContext) = {
    Identifier(ctx.getText)
  }


}

object ParseBuddy {

  case class ParseFailure(error: String)

  def parse(input: String): Either[ParseFailure, QueryNoWith] = {
    val charStream = CharStreams.fromString(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    val node: Node = prestoVisitor.visit(parser.statement)
    val qnw = node.asInstanceOf[QueryNoWith]
    if (qnw == null) Left(ParseFailure("oops")) else Right(qnw)
  }

}

object ParseBuddyApp extends App {
  import ca.valencik.bigsqlparse.ParseBuddy._

  private val exitCommands = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def inputLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("ParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      println(parse(inputQuery))
      inputLoop()
    }
  }

  inputLoop()
}
