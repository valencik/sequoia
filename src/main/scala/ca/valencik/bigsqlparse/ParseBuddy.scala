package ca.valencik.bigsqlparse

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.TerminalNode
import scala.util.{ Failure, Success, Try }

import scala.collection.JavaConversions._


sealed trait Node
case class NodeMessage(text: String) extends Node
case class NodeLocation(line: Int, post: Int)
object NodeLocation {
  def apply(token: Token): NodeLocation = NodeLocation(token.getLine, token.getCharPositionInLine)
}
case class SelectItem(location: NodeLocation) extends Node
case class SingleColumn(identifier: String, expression: Expression, location: NodeLocation) extends Node

case class Select(selectItems: List[String], location: NodeLocation) extends Node
object Select {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): Select = {
    Select(ctx.selectItem.map(_.getText).toList, NodeLocation(ctx.getStart))
  }
}
case class Relation(name: String, location: NodeLocation) extends Node
case class From(relations: List[Relation]) extends Node
object From {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): From = {
    val rs = ctx.relation.map { case ctx =>
      Relation(ctx.getText, NodeLocation(ctx.getStart))
    }.toList
    From(rs)
  }
}
case class Expression(name: String, location: NodeLocation) extends Node
case class Where(expression: Option[Expression]) extends Node
object Where {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): Where = {
    lazy val w = Expression(ctx.where.getText, NodeLocation(ctx.where.start))
    Where(Try(w).toOption)
  }
}
case class GroupingElement(groupingSet: List[Expression]) extends Node
object GroupingElement {
  def apply(ctx: SqlBaseParser.GroupingElementContext): GroupingElement = {
    GroupingElement(List(Expression(ctx.getText, NodeLocation(ctx.getStart))))
  }
}
case class GroupBy(groupingElements: List[GroupingElement]) extends Node
object GroupBy {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): GroupBy = {
    val g: List[GroupingElement] = Try(ctx.groupBy.groupingElement) match {
      case Success(gs) => gs.map(GroupingElement(_)).toList
      case Failure(_) => List()
    }
    GroupBy(g)
  }
}
case class OrderBy(location: NodeLocation) extends Node
case class Limit(location: NodeLocation) extends Node
case class QuerySpecification(
  select: Select,
  from: From,
  where: Where,
  groupBy: GroupBy,
  having: Option[Expression],
  orderBy: Option[OrderBy],
  limit: Option[Limit]
) extends Node
object QuerySpecification {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): QuerySpecification = {
    QuerySpecification(
      Select(ctx),
      From(ctx),
      Where(ctx),
      GroupBy(ctx),
      None, None, None)
  }
}

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Node] {

  override def visitQuery(ctx: SqlBaseParser.QueryContext) = visitQueryNoWith(ctx.queryNoWith)

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext) = visitQueryTerm(ctx.queryTerm)

  def visitQueryTerm(ctx: SqlBaseParser.QueryTermContext) = {
    val qp = ctx.asInstanceOf[SqlBaseParser.QueryTermDefaultContext].queryPrimary
    visitQueryPrimary(qp)
  }

  def visitQueryPrimary(ctx: SqlBaseParser.QueryPrimaryContext) = {
    val qs = ctx.asInstanceOf[SqlBaseParser.QueryPrimaryDefaultContext].querySpecification
    visitQuerySpecification(qs)
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext) = {
    QuerySpecification(ctx)
  }

}

object ParseBuddy {

  def parse(input: String): QuerySpecification = {
    val charStream = new ANTLRInputStream(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    prestoVisitor.visitQuery(parser.query)
  }

}

object ParseBuddyApp extends App {
  import ca.valencik.bigsqlparse.ParseBuddy._

}
