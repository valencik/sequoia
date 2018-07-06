package ca.valencik.bigsqlparse

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.TerminalNode
import scala.util.{ Failure, Success, Try }

import scala.collection.JavaConversions._


sealed trait Node
case class NodeLocation(line: Int, pos: Int) {
  override def toString = s"[${line},${pos}]"
}
case class Name(text: String, location: NodeLocation) extends Node with Relation
case class SelectItem(location: NodeLocation) extends Node
case class Select(selectItems: List[String], location: NodeLocation) extends Node

sealed trait Relation
case class From(relations: List[Relation]) extends Node

sealed trait Expression
case class Identifier(name: String, location: NodeLocation) extends Node with Expression
case class BooleanExpression(left: Expression, op: Operator, right: Expression) extends Node with Expression
case class ComparisonExpression(left: Expression, op: Comparison, right: Expression) extends Node with Expression
case class IsNullPredicate(value: Expression, location: NodeLocation) extends Node with Expression
case class IsNotNullPredicate(value: Expression, location: NodeLocation) extends Node with Expression

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


object NodeLocation {
  def apply(token: Token): NodeLocation =
    NodeLocation(token.getLine, token.getCharPositionInLine)
}
object Select {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): Select = {
    Select(ctx.selectItem.map(_.getText).toList, NodeLocation(ctx.getStart))
  }
}
object GroupingElement {
  def apply(ctx: SqlBaseParser.GroupingElementContext): GroupingElement = {
    GroupingElement(List(Identifier(ctx.getText, NodeLocation(ctx.getStart))))
  }
}
object GroupBy {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): GroupBy = {
    val g: List[GroupingElement] = Try(ctx.groupBy.groupingElement) match {
      case Success(gs) => gs.map(GroupingElement(_)).toList
      case Failure(_) => List()
    }
    GroupBy(g)
  }
}
object Having {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): Having = {
    lazy val e = Identifier(ctx.having.getText, NodeLocation(ctx.having.start))
    Having(Try(e).toOption)
  }
}
case class OrderBy(name: String, location: NodeLocation) extends Node
case class Limit(value: String, location: NodeLocation) extends Node
case class QueryNoWith(querySpecification: Option[QuerySpecification], orderBy: Option[OrderBy], limit: Option[Limit]) extends Node
object QueryNoWith {
  def apply(qso: Option[QuerySpecification], ctx: SqlBaseParser.QueryNoWithContext): QueryNoWith = {
    val orderBy = Try(OrderBy(ctx.sortItem.map{_.expression.getText}.mkString, NodeLocation(ctx.ORDER.getSymbol))).toOption
    val limit = Try(Limit(ctx.limit.getText, NodeLocation(ctx.limit))).toOption
    QueryNoWith(qso, orderBy, limit)
  }
}

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
case class Join(jointype: JoinType, left: Relation, right: Relation, location: NodeLocation, criterea: Option[JoinCriteria]) extends Node with Relation
case class SampledRelation(text: String, location: NodeLocation) extends Node with Relation

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
          case ic => Identifier(ic.getText, NodeLocation(ic.getStart))
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

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext) = {
    val qso = visit(ctx.queryTerm).asInstanceOf[QuerySpecification]
    QueryNoWith(Some(qso), ctx)
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext) = {
    val select = Select(ctx.selectItem.map(_.getText).toList, NodeLocation(ctx.getStart))
    val from = From(ctx.relation.map(visit(_).asInstanceOf[Relation]).toList)
    val where = Where(if (ctx.where != null) Some(visit(ctx.where).asInstanceOf[Expression]) else None)
    val groupBy = GroupBy(ctx)
    val having = Having(ctx)
    QuerySpecification(select, from, where, groupBy, having)
  }

  override def visitJoinRelation(ctx: SqlBaseParser.JoinRelationContext) = {
    val left = visit(ctx.left).asInstanceOf[Relation]
    val right = getRight(ctx)
    val joinType = getJoinType(ctx)
    val joinCriteria = getJoinCriteria(ctx)
    Join(joinType, left, right, NodeLocation(ctx.getStart), joinCriteria)
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
      IsNotNullPredicate(exp, NodeLocation(ctx.getStart))
    else
      IsNullPredicate(exp, NodeLocation(ctx.getStart))
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
    Identifier(ctx.getText, NodeLocation(ctx.getStart))
  }
  override def visitQualifiedName(ctx: SqlBaseParser.QualifiedNameContext) = {
    Name(ctx.getText, NodeLocation(ctx.getStart))
  }
  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext) = {
    Identifier(ctx.getText, NodeLocation(ctx.getStart))
  }
  override def visitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext) = {
    Identifier(ctx.getText, NodeLocation(ctx.getStart))
  }
  override def visitBackQuotedIdentifier(ctx: SqlBaseParser.BackQuotedIdentifierContext) = {
    Identifier(ctx.getText, NodeLocation(ctx.getStart))
  }
  override def visitDigitIdentifier(ctx: SqlBaseParser.DigitIdentifierContext) = {
    Identifier(ctx.getText, NodeLocation(ctx.getStart))
  }


}

object ParseBuddy {

  def parse(input: String): Node = {
    val charStream = CharStreams.fromString(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    prestoVisitor.visit(parser.statement)
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
