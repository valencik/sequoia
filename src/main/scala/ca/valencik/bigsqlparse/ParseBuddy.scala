package ca.valencik.bigsqlparse

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.TerminalNode
import scala.util.Try

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
case class Expression(location: NodeLocation) extends Node
case class GroupBy(location: NodeLocation) extends Node
case class OrderBy(location: NodeLocation) extends Node
case class Limit(location: NodeLocation) extends Node
case class QuerySpecification(
  select: Select,
  from: Option[From],
  where: Option[Expression],
  groupBy: Option[GroupBy],
  having: Option[Expression],
  orderBy: Option[OrderBy],
  limit: Option[Limit]
) extends Node
object QuerySpecification {
  def apply(ctx: SqlBaseParser.QuerySpecificationContext): QuerySpecification = {
    QuerySpecification(
      Select(ctx),
      Some(From(ctx)),
      None, None, None, None, None)
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
    println("Called visitQuerySpecification")
    println("children size:", ctx.children.size)
    println("children iter:", ctx.children.iterator.map(_.getText).toList)
    println("selectItem: ", ctx.selectItem.map(_.getText).toList)
    println("relations: ", ctx.relation.map(_.getText).toList)
    println("select: ", ctx.SELECT.getSymbol.getText)
    println("from: ", ctx.FROM.getSymbol.getText)
    println("where (null): ", ctx.WHERE)
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

  val exp: String            = "SELECT name, COUNT(*) FROM bar"
  val parsedExp: Node  = parse(exp)
  println(exp)
  println(parsedExp)
}
